package connector

import (
	"context"
	"crypto/tls"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/conductorone/baton-servicenow/pkg/incremental"
	"github.com/conductorone/baton-servicenow/pkg/servicenow"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
)

var (
	resourceTypeUser = &v2.ResourceType{
		Id:          "user",
		DisplayName: "User",
		Traits: []v2.ResourceType_Trait{
			v2.ResourceType_TRAIT_USER,
		},
		Annotations: annotationsForUserResourceType(),
	}
	resourceTypeRole = &v2.ResourceType{
		Id:          "role",
		DisplayName: "Role",
		Traits: []v2.ResourceType_Trait{
			v2.ResourceType_TRAIT_ROLE,
		},
	}
	resourceTypeGroup = &v2.ResourceType{
		Id:          "group",
		DisplayName: "Group",
		Traits: []v2.ResourceType_Trait{
			v2.ResourceType_TRAIT_GROUP,
		},
	}
)

type ServiceNow struct {
	client *servicenow.Client
	state  *incremental.State
}

func (s *ServiceNow) ResourceSyncers(ctx context.Context) []connectorbuilder.ResourceSyncer {
	return []connectorbuilder.ResourceSyncer{
		userBuilder(s.client, s.state),
		roleBuilder(s.client, s.state),
		groupBuilder(s.client, s.state),
	}
}

// Close persists the incremental-sync snapshot. The connectorbuilder invokes
// Close after a successful sync, which is the only safe moment to advance the
// watermark (persisting on a partial/failed run could skip rows). It is a
// no-op when incremental mode is disabled.
func (s *ServiceNow) Close(ctx context.Context) error {
	if s.state == nil {
		return nil
	}
	if err := s.state.Save(); err != nil {
		return fmt.Errorf("baton-servicenow: failed to persist incremental state: %w", err)
	}
	return nil
}

func (s *ServiceNow) Metadata(ctx context.Context) (*v2.ConnectorMetadata, error) {
	return &v2.ConnectorMetadata{
		DisplayName: "ServiceNow",
		Description: "Connector to sync users to ServiceNow",
		AccountCreationSchema: &v2.ConnectorAccountCreationSchema{
			FieldMap: map[string]*v2.ConnectorAccountCreationSchema_Field{
				"username": {
					DisplayName: "Username",
					Required:    true,
					Description: "Username of the user",
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
					Placeholder: "John08",
					Order:       1,
				},
				"email": {
					DisplayName: "Email",
					Required:    true,
					Description: "Email address of the user",
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
					Placeholder: "user@example.com",
					Order:       2,
				},
				"first_name": {
					DisplayName: "First Name",
					Required:    true,
					Description: "User's first name",
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
					Placeholder: "John",
					Order:       3,
				},
				"last_name": {
					DisplayName: "Last Name",
					Required:    true,
					Description: "User's last name",
					Field: &v2.ConnectorAccountCreationSchema_Field_StringField{
						StringField: &v2.ConnectorAccountCreationSchema_StringField{},
					},
					Placeholder: "Travolta",
					Order:       4,
				},
			},
		},
	}, nil
}

// Validates that we have credentials and an endpoint. Does not validate that the credentials have all of the correct permissions.
func (s *ServiceNow) Validate(ctx context.Context) (annotations.Annotations, error) {
	pagination := servicenow.PaginationVars{
		Limit: 1,
	}

	_, _, err := s.client.GetUsers(ctx, pagination)
	if err != nil {
		return nil, fmt.Errorf("baton-servicenow: current user is not able to list users: %w", err)
	}

	return nil, nil
}

// New returns the ServiceNow connector.
//
// When incremental is true, syncs after the first only fetch records whose
// sys_updated_on is at or after the watermark stored in the connector-managed
// state file under stateDir (keyed by deployment), merging the deltas over the
// cached snapshot so the c1z stays complete. When false, every sync is a full
// pull and no state is read or written.
func New(
	ctx context.Context, auth string, deployment string, ticketSchemaFilters map[string]string,
	allowedDomains []string, customUserFields []string, baseURL string, insecure bool,
	incrementalEnabled bool, stateDir string,
) (*ServiceNow, error) {
	uhttpOpts := []uhttp.Option{uhttp.WithLogger(true, ctxzap.Extract(ctx))}
	if insecure {
		uhttpOpts = append(uhttpOpts, uhttp.WithTLSClientConfig(&tls.Config{InsecureSkipVerify: true})) //nolint:gosec // G402: intentional for testing with self-signed certs
	}
	httpClient, err := uhttp.NewClient(ctx, uhttpOpts...)
	if err != nil {
		return nil, err
	}

	servicenowClient, err := servicenow.NewClient(httpClient, auth, deployment, ticketSchemaFilters, allowedDomains, customUserFields, baseURL)
	if err != nil {
		return nil, err
	}

	state, err := incremental.Load(stateDir, deployment, incrementalEnabled, servicenowClient)
	if err != nil {
		return nil, fmt.Errorf("baton-servicenow: failed to load incremental state: %w", err)
	}

	return &ServiceNow{
		client: servicenowClient,
		state:  state,
	}, nil
}
