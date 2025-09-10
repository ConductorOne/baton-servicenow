package connector

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-servicenow/pkg/servicenow"
)

type userResourceType struct {
	resourceType *v2.ResourceType
	client       *servicenow.Client
}

func (u *userResourceType) ResourceType(_ context.Context) *v2.ResourceType {
	return u.resourceType
}

// Create a new connector resource for an ServiceNow User.
func userResource(user *servicenow.User) (*v2.Resource, error) {
	profile := map[string]interface{}{
		"login":      user.UserName,
		"user_id":    user.Id,
		"user_roles": user.Roles,
		"first_name": user.FirstName,
		"last_name":  user.LastName,
		"active":     user.Active,
	}

	// Map ServiceNow active status to Baton user status
	var userStatus v2.UserTrait_Status_Status
	switch user.Active {
	case "true", "True", "TRUE", "1":
		userStatus = v2.UserTrait_Status_STATUS_ENABLED
	case "false", "False", "FALSE", "0":
		userStatus = v2.UserTrait_Status_STATUS_DISABLED
	default:
		// Default to disabled for unknown values to be safe
		userStatus = v2.UserTrait_Status_STATUS_DISABLED
	}

	userTraitOptions := []rs.UserTraitOption{
		rs.WithEmail(user.Email, true),
		rs.WithUserProfile(profile),
		rs.WithStatus(userStatus),
	}

	resource, err := rs.NewUserResource(
		user.UserName,
		resourceTypeUser,
		user.Id,
		userTraitOptions,
	)

	if err != nil {
		return nil, err
	}

	return resource, nil
}

func (u *userResourceType) List(ctx context.Context, _ *v2.ResourceId, pt *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	bag, offset, err := parsePageToken(pt.Token, &v2.ResourceId{ResourceType: resourceTypeUser.Id})
	if err != nil {
		return nil, "", nil, err
	}

	users, nextPageToken, err := u.client.GetUsers(
		ctx,
		servicenow.PaginationVars{
			Limit:  ResourcesPageSize,
			Offset: offset,
		},
		nil,
	)
	if err != nil {
		return nil, "", nil, fmt.Errorf("servicenow-connector: failed to list users: %w", err)
	}

	nextPage, err := bag.NextToken(nextPageToken)
	if err != nil {
		return nil, "", nil, err
	}

	var rv []*v2.Resource
	for _, user := range users {
		userCopy := user
		ur, err := userResource(&userCopy)

		if err != nil {
			return nil, "", nil, err
		}

		rv = append(rv, ur)
	}

	return rv, nextPage, nil, nil
}

func (u *userResourceType) Entitlements(ctx context.Context, resource *v2.Resource, token *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func (u *userResourceType) Grants(ctx context.Context, resource *v2.Resource, token *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	return nil, "", nil, nil
}

func userBuilder(client *servicenow.Client) *userResourceType {
	return &userResourceType{
		resourceType: resourceTypeUser,
		client:       client,
	}
}

// CreateAccountCapabilityDetails returns the account provisioning capabilities of this connector.
// In this case, only account creation without password is supported.
func (u *userResourceType) CreateAccountCapabilityDetails(
	_ context.Context,
) (*v2.CredentialDetailsAccountProvisioning, annotations.Annotations, error) {
	return &v2.CredentialDetailsAccountProvisioning{
		SupportedCredentialOptions: []v2.CapabilityDetailCredentialOption{
			v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_NO_PASSWORD,
		},
		PreferredCredentialOption: v2.CapabilityDetailCredentialOption_CAPABILITY_DETAIL_CREDENTIAL_OPTION_NO_PASSWORD,
	}, nil, nil
}

func (u *userResourceType) CreateAccount(
	ctx context.Context,
	accountInfo *v2.AccountInfo,
	_ *v2.CredentialOptions,
) (connectorbuilder.CreateAccountResponse, []*v2.PlaintextData,
	annotations.Annotations,
	error) {
	profile := accountInfo.GetProfile().AsMap()
	if profile == nil {
		return nil, nil, nil, fmt.Errorf("missing profile in CreateAccountRequest")
	}

	userName, ok := profile["username"].(string)
	if !ok || userName == "" {
		return nil, nil, nil, fmt.Errorf("missing or invalid 'userName' in profile")
	}

	email, ok := profile["email"].(string)
	if !ok || email == "" {
		return nil, nil, nil, fmt.Errorf("missing or invalid 'email' in profile")
	}
	firstName, ok := profile["first_name"].(string)
	if !ok || firstName == "" {
		return nil, nil, nil, fmt.Errorf("missing or invalid 'first_name' in profile")
	}
	lastName, ok := profile["last_name"].(string)
	if !ok || lastName == "" {
		return nil, nil, nil, fmt.Errorf("missing or invalid 'last_name' in profile")
	}

	user := map[string]string{
		"user_name":  userName,
		"first_name": firstName,
		"last_name":  lastName,
		"email":      email,
		"active":     "true",
	}

	createdUser, err := u.client.CreateUserAccount(ctx, user)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create user in ServiceNow: %w", err)
	}

	resource, err := userResource(createdUser)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create Baton resource: %w", err)
	}

	return &v2.CreateAccountResponse_SuccessResult{Resource: resource}, nil, nil, nil
}
