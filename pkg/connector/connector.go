package connector

import (
	"context"
	"crypto/tls"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/conductorone/baton-servicenow/pkg/servicenow"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
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
	// ticketSchemaIDs is the exclusive set of well-formed catalog item IDs
	// offered as ticket schemas, in configured order. Set once in New and never
	// mutated, so concurrent list calls can share it.
	ticketSchemaIDs []string
	// ticketSchemaIDsSelected records that ticket-schema-ids was configured, even
	// if every entry was blank or malformed: an explicit selection that yields
	// nothing must return no schemas, never fall back to broad discovery.
	ticketSchemaIDsSelected bool
	// ticketSchemaIDCountErr is the InvalidArgument error for a selection over
	// MaxTicketSchemaIDs, or nil. Computed once in New and returned by the
	// selected-IDs listing, so Validate and resource sync are not blocked by it.
	ticketSchemaIDCountErr error
}

func (s *ServiceNow) ResourceSyncers(ctx context.Context) []connectorbuilder.ResourceSyncer {
	return []connectorbuilder.ResourceSyncer{
		userBuilder(s.client),
		roleBuilder(s.client),
		groupBuilder(s.client),
	}
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
	pagination := servicenow.KeysetPaginationVars{
		Limit: 1,
	}

	_, _, annos, err := s.client.GetUsers(ctx, pagination)
	if err != nil {
		return annos, fmt.Errorf("baton-servicenow: current user is not able to list users: %w", err)
	}

	return annos, nil
}

// New returns the ServiceNow connector.
func New(
	ctx context.Context, auth string, deployment string, ticketSchemaFilters map[string]string,
	ticketSchemaIDs []string, allowedDomains []string, customUserFields []string, baseURL string, insecure bool, twoStepCheckout bool,
) (*ServiceNow, error) {
	uhttpOpts := []uhttp.Option{uhttp.WithLogger(true, ctxzap.Extract(ctx))}
	if insecure {
		uhttpOpts = append(uhttpOpts, uhttp.WithTLSClientConfig(&tls.Config{InsecureSkipVerify: true})) //nolint:gosec // G402: intentional for testing with self-signed certs
	}
	httpClient, err := uhttp.NewClient(ctx, uhttpOpts...)
	if err != nil {
		return nil, err
	}

	// BaseHttpClient is what makes uhttp's DoOptions (notably
	// WithRatelimitData) available to the client layer.
	baseHttpClient, err := uhttp.NewBaseHttpClientWithContext(ctx, httpClient)
	if err != nil {
		return nil, err
	}

	servicenowClient, err := servicenow.NewClient(baseHttpClient, auth, deployment, ticketSchemaFilters, allowedDomains, customUserFields, baseURL)
	if err != nil {
		return nil, err
	}
	servicenowClient.TwoStepCheckout = twoStepCheckout

	configuredIDs := normalizeTicketSchemaIDs(ticketSchemaIDs)
	// Counted before malformed entries are filtered: the limit applies to what
	// the operator configured, and no remote call is needed to enforce it. It is
	// enforced by ListTicketSchemas, not here, so Validate and sync still work.
	countErr := validateTicketSchemaIDCount(configuredIDs)
	// A malformed ID must not fail Validate or sync: warn once here and omit it.
	// No remote lookup is made to check the rest.
	schemaIDs, malformed := splitTicketSchemaIDs(configuredIDs)
	l := ctxzap.Extract(ctx)
	for _, id := range malformed {
		l.Warn("baton-servicenow: ignoring configured ticket schema ID that is not a valid ServiceNow sys_id (letters, digits, '_' and '-' only)",
			zap.String("ticket_schema_id", loggableID(id)))
	}
	if len(ticketSchemaIDs) > 0 && len(schemaIDs) == 0 {
		l.Warn("baton-servicenow: none of the configured ticket schema IDs is usable; no ticket schemas will be offered",
			zap.Int("configured_ids", len(configuredIDs)))
	}

	return &ServiceNow{
		client:          servicenowClient,
		ticketSchemaIDs: schemaIDs,
		// Judged on the slice as provided, before normalization, to match config
		// validation: a blank entry counts as set, nil or empty as unset.
		ticketSchemaIDsSelected: len(ticketSchemaIDs) > 0,
		ticketSchemaIDCountErr:  countErr,
	}, nil
}
