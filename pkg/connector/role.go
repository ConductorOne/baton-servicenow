package connector

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-servicenow/pkg/servicenow"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

const roleMembership = "member"

type roleResourceType struct {
	resourceType *v2.ResourceType
	client       *servicenow.Client
}

func (r *roleResourceType) ResourceType(_ context.Context) *v2.ResourceType {
	return r.resourceType
}

// Create a new connector resource for an ServiceNow Role.
func roleResource(role *servicenow.Role) (*v2.Resource, error) {
	profile := map[string]interface{}{
		"role_name": role.Name,
		"role_id":   role.Id,
	}

	roleTraitOptions := []rs.RoleTraitOption{
		rs.WithRoleProfile(profile),
	}

	resource, err := rs.NewRoleResource(
		role.Name,
		resourceTypeRole,
		role.Id,
		roleTraitOptions,
	)

	if err != nil {
		return nil, err
	}

	return resource, nil
}

func (r *roleResourceType) List(ctx context.Context, _ *v2.ResourceId, pt *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	bag, offset, err := parsePageToken(pt.Token, &v2.ResourceId{ResourceType: resourceTypeRole.Id})
	if err != nil {
		return nil, "", nil, err
	}

	roles, nextPageToken, err := r.client.GetRoles(
		ctx,
		servicenow.PaginationVars{
			Limit:  ResourcesPageSize,
			Offset: offset,
		},
	)
	if err != nil {
		return nil, "", nil, fmt.Errorf("servicenow-connector: failed to list roles: %w", err)
	}

	nextPage, err := bag.NextToken(nextPageToken)
	if err != nil {
		return nil, "", nil, err
	}

	var rv []*v2.Resource
	for _, role := range roles {
		roleCopy := role
		rr, err := roleResource(&roleCopy)

		if err != nil {
			return nil, "", nil, err
		}

		rv = append(rv, rr)
	}

	return rv, nextPage, nil, nil
}

func (r *roleResourceType) Entitlements(ctx context.Context, resource *v2.Resource, token *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	var rv []*v2.Entitlement

	assignmentOptions := []ent.EntitlementOption{
		ent.WithGrantableTo(resourceTypeUser, resourceTypeGroup),
		ent.WithDisplayName(fmt.Sprintf("%s Role %s", resource.DisplayName, roleMembership)),
		ent.WithDescription(fmt.Sprintf("Access to %s role in ServiceNow", resource.DisplayName)),
	}

	rv = append(rv, ent.NewAssignmentEntitlement(
		resource,
		roleMembership,
		assignmentOptions...,
	))

	return rv, "", nil, nil
}

func (r *roleResourceType) Grants(ctx context.Context, resource *v2.Resource, pt *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	bag, offset, err := parsePageToken(pt.Token, resource.Id)
	if err != nil {
		return nil, "", nil, err
	}

	var rv []*v2.Grant
	switch bag.ResourceTypeID() {
	case resourceTypeRole.Id:
		bag.Pop()
		bag.Push(pagination.PageState{
			ResourceTypeID: resourceTypeGroup.Id,
		})
		bag.Push(pagination.PageState{
			ResourceTypeID: resourceTypeUser.Id,
		})

	case resourceTypeUser.Id:
		usersToRoles, nextPageToken, err := r.client.GetUserToRole(
			ctx,
			"", // all users
			resource.Id.Resource,
			servicenow.PaginationVars{
				Limit:  ResourcesPageSize,
				Offset: offset,
			},
		)
		if err != nil {
			return nil, "", nil, fmt.Errorf("servicenow-connector: failed to list users under role %s: %w", resource.Id.Resource, err)
		}

		err = bag.Next(nextPageToken)
		if err != nil {
			return nil, "", nil, err
		}

		// for each roleBinding, create a grant
		for _, roleBinding := range usersToRoles {
			rv = append(
				rv,
				grant.NewGrant(
					resource,
					roleMembership,
					&v2.ResourceId{
						ResourceType: resourceTypeUser.Id,
						Resource:     roleBinding.User,
					},
				),
			)
		}

	case resourceTypeGroup.Id:
		groupsToRoles, nextPageToken, err := r.client.GetGroupToRole(
			ctx,
			"", // all groups
			resource.Id.Resource,
			servicenow.PaginationVars{
				Limit:  ResourcesPageSize,
				Offset: offset,
			},
		)
		if err != nil {
			return nil, "", nil, fmt.Errorf("servicenow-connector: failed to list groups under role %s: %w", resource.Id.Resource, err)
		}

		err = bag.Next(nextPageToken)
		if err != nil {
			return nil, "", nil, err
		}

		// for each roleBinding, create a grant
		for _, roleBinding := range groupsToRoles {
			rv = append(
				rv,
				grant.NewGrant(
					resource,
					roleMembership,
					&v2.ResourceId{
						ResourceType: resourceTypeGroup.Id,
						Resource:     roleBinding.Group,
					},
					r.helperGrantForGroup(roleBinding)...,
				),
			)
		}

	default:
		return nil, "", nil, fmt.Errorf("unknown resource type: %s", bag.ResourceTypeID())
	}

	nextPage, err := bag.Marshal()
	if err != nil {
		return nil, "", nil, err
	}

	return rv, nextPage, nil, nil
}

// This is a helper function to add heritance.
func (r *roleResourceType) helperGrantForGroup(role servicenow.GroupToRole) []grant.GrantOption {
	var grantOptions []grant.GrantOption

	grantOptions = append(grantOptions, grant.WithAnnotation(&v2.GrantExpandable{
		EntitlementIds: []string{fmt.Sprintf("group:%s:member", role.Group)},
		Shallow:        true,
	}))

	return grantOptions
}

func (r *roleResourceType) GrantToUser(ctx context.Context, l *zap.Logger, principal string, roleId string) (annotations.Annotations, error) {
	userRoles, _, err := r.client.GetUserToRole(
		ctx,
		principal,
		roleId,
		servicenow.PaginationVars{Limit: 1},
	)
	if err != nil {
		return nil, fmt.Errorf("servicenow-connector: failed to get user roles for %s: %w", principal, err)
	}

	// check if the user already has the role
	if len(userRoles) > 0 {
		l.Warn(
			"servicenow-connector: user already has specified role",
			zap.String("user", principal),
			zap.String("role", roleId),
		)

		return annotations.New(&v2.GrantAlreadyExists{}), nil
	}

	// grant the role to the user
	err = r.client.GrantRoleToUser(
		ctx,
		servicenow.UserToRolePayload{
			User: principal,
			Role: roleId,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("servicenow-connector: failed to grant role %s to user %s: %w", roleId, principal, err)
	}

	l.Debug("granted role to user", zap.String("role", roleId))
	return nil, nil
}

func (r *roleResourceType) GrantToGroup(ctx context.Context, l *zap.Logger, principal string, roleId string) (annotations.Annotations, error) {
	groupRoles, _, err := r.client.GetGroupToRole(
		ctx,
		principal,
		roleId,
		servicenow.PaginationVars{Limit: 1},
	)
	if err != nil {
		return nil, fmt.Errorf("servicenow-connector: failed to get group roles for %s: %w", principal, err)
	}

	// check if the group already has the role
	if len(groupRoles) > 0 {
		l.Warn(
			"servicenow-connector: group already has specified role",
			zap.String("group", principal),
			zap.String("role", roleId),
		)

		return nil, fmt.Errorf("servicenow-connector: group already has specified role")
	}

	// grant the role to the group
	err = r.client.GrantRoleToGroup(
		ctx,
		servicenow.GroupToRolePayload{
			Group: principal,
			Role:  roleId,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("servicenow-connector: failed to grant role %s to group %s: %w", roleId, principal, err)
	}

	l.Debug("granted role to group", zap.String("role", roleId))
	return nil, nil
}

func (r *roleResourceType) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	principalIsUser := principal.Id.ResourceType == resourceTypeUser.Id
	principalIsGroup := principal.Id.ResourceType == resourceTypeGroup.Id

	if !principalIsUser && !principalIsGroup {
		l.Warn(
			"servicenow-connector: only users or groups can be granted role membership",
			zap.String("principal_type", principal.Id.ResourceType),
			zap.String("principal_id", principal.Id.Resource),
		)

		return nil, fmt.Errorf("servicenow-connector: only users or groups can be granted role membership")
	}

	roleId := entitlement.Resource.Id.Resource

	if principalIsUser {
		return r.GrantToUser(ctx, l, principal.Id.Resource, roleId)
	}

	if principalIsGroup {
		return r.GrantToGroup(ctx, l, principal.Id.Resource, roleId)
	}

	return nil, nil
}

func (r *roleResourceType) RevokeFromUser(ctx context.Context, l *zap.Logger, principal *v2.Resource, roleId string) (annotations.Annotations, error) {
	// check if role is present
	userRoles, _, err := r.client.GetUserToRole(
		ctx,
		principal.Id.Resource,
		roleId,
		servicenow.PaginationVars{Limit: 1},
	)
	if err != nil {
		return nil, fmt.Errorf("servicenow-connector: failed to get user roles for %s: %w", principal.Id.Resource, err)
	}

	if len(userRoles) == 0 {
		l.Warn(
			"servicenow-connector: cannot revoke not existing role from user",
			zap.String("user", principal.Id.Resource),
			zap.String("role", roleId),
		)

		return annotations.New(&v2.GrantAlreadyRevoked{}), nil
	}

	// revoke all roles (inherited or not) from the user
	for _, userRole := range userRoles {
		err = r.client.RevokeRoleFromUser(
			ctx,
			userRole.Id,
		)
		if err != nil {
			return nil, fmt.Errorf("servicenow-connector: failed to revoke role %s from user %s: %w", roleId, principal.Id.Resource, err)
		}

		l.Debug("revoked role from user", zap.String("role", roleId))
	}

	return nil, nil
}

func (r *roleResourceType) RevokeFromGroup(ctx context.Context, l *zap.Logger, principal *v2.Resource, roleId string) (annotations.Annotations, error) {
	// check if role is present
	groupRoles, _, err := r.client.GetGroupToRole(
		ctx,
		principal.Id.Resource,
		roleId,
		servicenow.PaginationVars{Limit: 1},
	)
	if err != nil {
		return nil, fmt.Errorf("servicenow-connector: failed to get group roles for %s: %w", principal.Id.Resource, err)
	}

	if len(groupRoles) == 0 {
		l.Warn(
			"servicenow-connector: cannot revoke not existing role from group",
			zap.String("group", principal.Id.Resource),
			zap.String("role", roleId),
		)

		return nil, fmt.Errorf("servicenow-connector: cannot revoke not existing role from group")
	}

	// revoke all roles (inherited or not) from the group
	for _, groupRole := range groupRoles {
		err = r.client.RevokeRoleFromGroup(
			ctx,
			groupRole.Id,
		)
		if err != nil {
			return nil, fmt.Errorf("servicenow-connector: failed to revoke role %s from group %s: %w", roleId, principal.Id.Resource, err)
		}

		l.Debug("revoked role from group", zap.String("role", roleId))
	}

	return nil, nil
}

func (r *roleResourceType) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	entitlement := grant.Entitlement
	principal := grant.Principal
	principalIsUser := principal.Id.ResourceType == resourceTypeUser.Id
	principalIsGroup := principal.Id.ResourceType == resourceTypeGroup.Id

	if !principalIsUser && !principalIsGroup {
		l.Warn(
			"servicenow-connector: only users or groups can be revoked role membership",
			zap.String("principal_type", principal.Id.ResourceType),
			zap.String("principal_id", principal.Id.Resource),
		)

		return nil, fmt.Errorf("servicenow-connector: only users or groups can be revoked role membership")
	}

	roleId := entitlement.Resource.Id.Resource

	if principalIsUser {
		return r.RevokeFromUser(ctx, l, principal, roleId)
	}

	if principalIsGroup {
		return r.RevokeFromGroup(ctx, l, principal, roleId)
	}

	return nil, nil
}

func roleBuilder(client *servicenow.Client) *roleResourceType {
	return &roleResourceType{
		resourceType: resourceTypeRole,
		client:       client,
	}
}
