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

type groupResourceType struct {
	resourceType *v2.ResourceType
	client       *servicenow.Client
}

func (g *groupResourceType) ResourceType(_ context.Context) *v2.ResourceType {
	return g.resourceType
}

const groupMembership = "member"

// Create a new connector resource for an ServiceNow Group.
func groupResource(ctx context.Context, group *servicenow.Group) (*v2.Resource, error) {
	profile := map[string]interface{}{
		"group_name":        group.Name,
		"group_id":          group.Id,
		"group_description": group.Description,
	}

	groupTraitOptions := []rs.GroupTraitOption{
		rs.WithGroupProfile(profile),
	}

	resource, err := rs.NewGroupResource(
		group.Name,
		resourceTypeGroup,
		group.Id,
		groupTraitOptions,
	)

	if err != nil {
		return nil, err
	}

	return resource, nil
}

func (g *groupResourceType) List(ctx context.Context, _ *v2.ResourceId, pt *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	bag, offset, err := parsePageToken(pt.Token, &v2.ResourceId{ResourceType: resourceTypeGroup.Id})
	if err != nil {
		return nil, "", nil, err
	}

	groups, total, err := g.client.GetGroups(
		ctx,
		servicenow.PaginationVars{
			Limit:  ResourcesPageSize,
			Offset: offset,
		},
		nil,
	)
	if err != nil {
		return nil, "", nil, fmt.Errorf("servicenow-connector: failed to list groups: %w", err)
	}

	nextPage, err := handleNextPage(bag, offset+ResourcesPageSize)
	if err != nil {
		return nil, "", nil, err
	}

	var rv []*v2.Resource
	for _, group := range groups {
		groupCopy := group
		rr, err := groupResource(ctx, &groupCopy)

		if err != nil {
			return nil, "", nil, err
		}

		rv = append(rv, rr)
	}

	if (offset + len(groups)) == total {
		return rv, "", nil, nil
	}

	return rv, nextPage, nil, nil
}

func (g *groupResourceType) Entitlements(ctx context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	var rv []*v2.Entitlement

	assignmentOptions := []ent.EntitlementOption{
		ent.WithGrantableTo(resourceTypeUser),
		ent.WithDisplayName(fmt.Sprintf("%s Group %s", resource.DisplayName, groupMembership)),
		ent.WithDescription(fmt.Sprintf("Access to %s group in ServiceNow", resource.DisplayName)),
	}

	rv = append(rv, ent.NewAssignmentEntitlement(
		resource,
		groupMembership,
		assignmentOptions...,
	))

	return rv, "", nil, nil
}

func (g *groupResourceType) Grants(ctx context.Context, resource *v2.Resource, pt *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	bag, offset, err := parsePageToken(pt.Token, &v2.ResourceId{ResourceType: resourceTypeGroup.Id})
	if err != nil {
		return nil, "", nil, err
	}

	groupMembers, total, err := g.client.GetUserToGroup(
		ctx,
		"", // all users
		resource.Id.Resource,
		servicenow.PaginationVars{
			Limit:  ResourcesPageSize,
			Offset: offset,
		},
	)
	if err != nil {
		return nil, "", nil, fmt.Errorf("servicenow-connector: failed to list groupMembers: %w", err)
	}

	if len(groupMembers) == 0 {
		return nil, "", nil, nil
	}

	nextPage, err := handleNextPage(bag, offset+ResourcesPageSize)
	if err != nil {
		return nil, "", nil, err
	}

	memberIds := mapGroupMembers(groupMembers)
	targetMembers, _, err := g.client.GetUsers(
		ctx,
		servicenow.PaginationVars{
			Limit: len(memberIds),
		},
		memberIds,
	)
	if err != nil {
		return nil, "", nil, fmt.Errorf("servicenow-connector: failed to list members under group %s: %w", resource.Id.Resource, err)
	}

	var rv []*v2.Grant
	for _, member := range targetMembers {
		memberCopy := member
		ur, err := userResource(ctx, &memberCopy)
		if err != nil {
			return nil, "", nil, err
		}

		// grant group membership
		rv = append(
			rv,
			grant.NewGrant(
				resource,
				groupMembership,
				ur.Id,
			),
		)
	}

	if (offset + len(groupMembers)) == total {
		return rv, "", nil, nil
	}

	return rv, nextPage, nil, nil
}

func (r *groupResourceType) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	if principal.Id.ResourceType != resourceTypeUser.Id {
		l.Warn(
			"baton-servicenow: only users can have group membership granted",
			zap.String("principal_type", principal.Id.ResourceType),
			zap.String("principal_id", principal.Id.Resource),
		)

		return nil, nil
	}

	groupId := entitlement.Resource.Id.Resource
	groupMembers, _, err := r.client.GetUserToGroup(
		ctx,
		principal.Id.Resource,
		groupId,
		servicenow.PaginationVars{Limit: 1},
	)
	if err != nil {
		return nil, fmt.Errorf("servicenow-connector: failed to get group members for %s: %w", entitlement.Id, err)
	}

	// check if user is already a member of the group
	if len(groupMembers) > 0 {
		l.Warn(
			"baton-servicenow: cannot add user who already is a member of the group",
			zap.String("group", entitlement.Id),
			zap.String("user", principal.Id.Resource),
		)

		return nil, fmt.Errorf("servicenow-connector: cannot add user who already is a member of the group")
	}

	// grant group membership to the user
	err = r.client.AddUserToGroup(
		ctx,
		servicenow.GroupMemberPayload{
			User:  principal.Id.Resource,
			Group: groupId,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("servicenow-connector: failed to add user %s to group %s: %w", principal.Id.Resource, groupId, err)
	}

	return nil, nil
}

func (r *groupResourceType) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	entitlement := grant.Entitlement
	principal := grant.Principal

	if principal.Id.ResourceType != resourceTypeUser.Id {
		l.Warn(
			"baton-servicenow: only users can have group membership revoked",
			zap.String("principal_type", principal.Id.ResourceType),
			zap.String("principal_id", principal.Id.Resource),
		)
	}

	groupId := entitlement.Resource.Id.Resource
	groupMembers, _, err := r.client.GetUserToGroup(
		ctx,
		principal.Id.Resource,
		groupId,
		servicenow.PaginationVars{Limit: 1},
	)
	if err != nil {
		return nil, fmt.Errorf("servicenow-connector: failed to get user roles for %s: %w", grant.Principal.Id.Resource, err)
	}

	// check if group is empty
	if len(groupMembers) == 0 {
		l.Warn(
			"baton-servicenow: cannot remove user from group they are not a member of",
			zap.String("group", entitlement.Id),
			zap.String("user", principal.Id.Resource),
		)

		return nil, fmt.Errorf("servicenow-connector: cannot remove user from group they are not a member of")
	}

	// revoke all group memberships from the user
	for _, grpMember := range groupMembers {
		err = r.client.RemoveUserFromGroup(
			ctx,
			grpMember.Id,
		)
		if err != nil {
			return nil, fmt.Errorf("servicenow-connector: failed to remove user %s from group: %w", grant.Principal.Id.Resource, err)
		}

		l.Debug("revoked role from user", zap.String("role", grant.Entitlement.Id))
	}

	return nil, nil
}

func groupBuilder(client *servicenow.Client) *groupResourceType {
	return &groupResourceType{
		resourceType: resourceTypeGroup,
		client:       client,
	}
}
