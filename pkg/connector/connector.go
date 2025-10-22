package connector

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
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
	client      *servicenow.Client
	userFilters []string
}

func (s *ServiceNow) ResourceSyncers(ctx context.Context) []connectorbuilder.ResourceSyncer {
	return []connectorbuilder.ResourceSyncer{
		userBuilder(s.client, s.userFilters),
		roleBuilder(s.client),
		groupBuilder(s.client),
	}
}

func (s *ServiceNow) Actions(ctx context.Context) (connectorbuilder.CustomActionManager, error) {
	return s.RegisterActionManager(ctx)
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

	_, _, err := s.client.GetUsers(ctx, pagination, nil, s.userFilters)
	if err != nil {
		return nil, fmt.Errorf("servicenow-connector: current user is not able to list users: %w", err)
	}

	return nil, nil
}

// New returns the ServiceNow connector.
func New(ctx context.Context, auth string, deployment string, ticketSchemaFilters map[string]string, allowedDomains []string) (*ServiceNow, error) {
	httpClient, err := uhttp.NewClient(ctx, uhttp.WithLogger(true, ctxzap.Extract(ctx)))
	if err != nil {
		return nil, err
	}

	servicenowClient, err := servicenow.NewClient(httpClient, auth, deployment, ticketSchemaFilters)
	if err != nil {
		return nil, err
	}

	return &ServiceNow{
		client:      servicenowClient,
		userFilters: allowedDomains,
	}, nil
}
