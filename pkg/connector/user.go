package connector

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
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
func userResource(ctx context.Context, user *servicenow.User) (*v2.Resource, error) {
	profile := map[string]interface{}{
		"login":      user.UserName,
		"user_id":    user.Id,
		"user_roles": user.Roles,
		"first_name": user.FirstName,
		"last_name":  user.LastName,
	}

	userTraitOptions := []rs.UserTraitOption{
		rs.WithEmail(user.Email, true),
		rs.WithUserProfile(profile),
		rs.WithStatus(v2.UserTrait_Status_STATUS_ENABLED),
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

	users, total, err := u.client.GetUsers(
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

	nextPage, err := handleNextPage(bag, offset+ResourcesPageSize)
	if err != nil {
		return nil, "", nil, err
	}

	var rv []*v2.Resource
	for _, user := range users {
		userCopy := user
		ur, err := userResource(ctx, &userCopy)

		if err != nil {
			return nil, "", nil, err
		}

		rv = append(rv, ur)
	}

	if (offset + len(users)) == total {
		return rv, "", nil, nil
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
