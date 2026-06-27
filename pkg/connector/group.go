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
	"github.com/conductorone/baton-servicenow/pkg/incremental"
	"github.com/conductorone/baton-servicenow/pkg/servicenow"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type groupResourceType struct {
	resourceType *v2.ResourceType
	client       *servicenow.Client
	state        *incremental.State
}

func (g *groupResourceType) ResourceType(_ context.Context) *v2.ResourceType {
	return g.resourceType
}

const groupMembership = "member"

// Create a new connector resource for an ServiceNow Group.
func groupResource(group *servicenow.Group) (*v2.Resource, error) {
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
	// Incremental path: drain only groups changed since the watermark, merge
	// over the cached snapshot, and emit the full union in one page.
	if g.state.Enabled() {
		g.state.Reconcile(ctx)
		changed, err := g.client.GetAllGroupsUpdatedSince(ctx, g.state.Watermark(incremental.StreamGroups))
		if err != nil {
			g.state.MarkFailed()
			return nil, "", nil, fmt.Errorf("baton-servicenow: failed to list groups (incremental): %w", err)
		}
		merged, err := g.state.MergeGroups(changed)
		if err != nil {
			g.state.MarkFailed()
			return nil, "", nil, fmt.Errorf("baton-servicenow: failed to persist groups state: %w", err)
		}
		rv, err := groupsToResources(merged)
		if err != nil {
			g.state.MarkFailed()
			return nil, "", nil, err
		}
		return rv, "", nil, nil
	}

	bag, offset, err := parsePageToken(pt.Token, &v2.ResourceId{ResourceType: resourceTypeGroup.Id})
	if err != nil {
		return nil, "", nil, err
	}

	groups, nextPageToken, err := g.client.GetGroups(
		ctx,
		servicenow.PaginationVars{
			Limit:  ResourcesPageSize,
			Offset: offset,
		},
		nil,
	)
	if err != nil {
		return nil, "", nil, fmt.Errorf("baton-servicenow: failed to list groups: %w", err)
	}

	nextPage, err := bag.NextToken(nextPageToken)
	if err != nil {
		return nil, "", nil, err
	}

	rv, err := groupsToResources(groups)
	if err != nil {
		return nil, "", nil, err
	}

	return rv, nextPage, nil, nil
}

func groupsToResources(groups []servicenow.Group) ([]*v2.Resource, error) {
	var rv []*v2.Resource
	for _, group := range groups {
		groupCopy := group
		rr, err := groupResource(&groupCopy)
		if err != nil {
			return nil, err
		}
		rv = append(rv, rr)
	}
	return rv, nil
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
	// Incremental path: drain only membership rows for this group changed since
	// the watermark, merge over the cached snapshot for this group, and emit the
	// full union of memberships in one page.
	if g.state.Enabled() {
		g.state.Reconcile(ctx)
		changed, err := g.client.GetAllUserToGroupUpdatedSince(ctx, resource.Id.Resource, g.state.Watermark(incremental.StreamGroupMembers))
		if err != nil {
			g.state.MarkFailed()
			return nil, "", nil, fmt.Errorf("baton-servicenow: failed to list groupMembers (incremental): %w", err)
		}
		merged, err := g.state.MergeGroupMembers(resource.Id.Resource, changed)
		if err != nil {
			g.state.MarkFailed()
			return nil, "", nil, fmt.Errorf("baton-servicenow: failed to persist group members state: %w", err)
		}
		rv, err := groupMembersToGrants(resource, mapGroupMembers(merged))
		if err != nil {
			g.state.MarkFailed()
			return nil, "", nil, err
		}
		return rv, "", nil, nil
	}

	bag, offset, err := parsePageToken(pt.Token, &v2.ResourceId{ResourceType: resourceTypeGroup.Id})
	if err != nil {
		return nil, "", nil, err
	}

	groupMembers, nextPageToken, err := g.client.GetUserToGroup(
		ctx,
		"", // all users
		resource.Id.Resource,
		servicenow.PaginationVars{
			Limit:  ResourcesPageSize,
			Offset: offset,
		},
	)
	if err != nil {
		return nil, "", nil, fmt.Errorf("baton-servicenow: failed to list groupMembers: %w", err)
	}

	nextPage, err := bag.NextToken(nextPageToken)
	if err != nil {
		return nil, "", nil, err
	}

	memberIDs := mapGroupMembers(groupMembers)
	if len(memberIDs) == 0 {
		return []*v2.Grant{}, nextPageToken, nil, nil
	}

	rv, err := groupMembersToGrants(resource, memberIDs)
	if err != nil {
		return nil, "", nil, err
	}

	return rv, nextPage, nil, nil
}

func groupMembersToGrants(resource *v2.Resource, memberIDs []string) ([]*v2.Grant, error) {
	var rv []*v2.Grant
	for _, member := range memberIDs {
		rID, err := rs.NewResourceID(resourceTypeUser, member)
		if err != nil {
			return nil, fmt.Errorf("baton-servicenow: error creating principal id")
		}
		rv = append(rv, grant.NewGrant(resource, groupMembership, rID))
	}
	return rv, nil
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
		return nil, fmt.Errorf("baton-servicenow: failed to get group members for %s: %w", entitlement.Id, err)
	}

	// check if user is already a member of the group
	if len(groupMembers) > 0 {
		l.Warn(
			"baton-servicenow: cannot add user who already is a member of the group",
			zap.String("group", entitlement.Id),
			zap.String("user", principal.Id.Resource),
		)

		return annotations.New(&v2.GrantAlreadyExists{}), nil
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
		return nil, fmt.Errorf("baton-servicenow: failed to add user %s to group %s: %w", principal.Id.Resource, groupId, err)
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
		return nil, fmt.Errorf("baton-servicenow: failed to get user roles for %s: %w", grant.Principal.Id.Resource, err)
	}

	// check if group is empty
	if len(groupMembers) == 0 {
		l.Warn(
			"baton-servicenow: cannot remove user from group they are not a member of",
			zap.String("group", entitlement.Id),
			zap.String("user", principal.Id.Resource),
		)

		return annotations.New(&v2.GrantAlreadyRevoked{}), nil
	}

	// revoke all group memberships from the user
	for _, grpMember := range groupMembers {
		err = r.client.RemoveUserFromGroup(
			ctx,
			grpMember.Id,
		)
		if err != nil {
			return nil, fmt.Errorf("baton-servicenow: failed to remove user %s from group: %w", grant.Principal.Id.Resource, err)
		}

		l.Debug("revoked role from user", zap.String("role", grant.Entitlement.Id))
	}

	return nil, nil
}

func groupBuilder(client *servicenow.Client, state *incremental.State) *groupResourceType {
	return &groupResourceType{
		resourceType: resourceTypeGroup,
		client:       client,
		state:        state,
	}
}
