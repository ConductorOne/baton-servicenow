package connector

import (
	"context"
	"fmt"

	"github.com/ConductorOne/baton-servicenow/pkg/servicenow"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
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

	groups, err := g.client.GetGroups(
		ctx,
		servicenow.PaginationVars{
			Limit:  ResourcesPageSize,
			Offset: offset,
		},
	)
	if err != nil {
		return nil, "", nil, fmt.Errorf("servicenow-connector: failed to list groups: %w", err)
	}

	if len(groups) == 0 {
		return nil, "", nil, nil
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

	if len(groups) < ResourcesPageSize {
		return rv, "", nil, nil
	}

	return rv, nextPage, nil, nil
}

func (g *groupResourceType) Entitlements(ctx context.Context, resource *v2.Resource, token *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
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

	// TODO: add entitlements for roles that could be shared throug groups

	return rv, "", nil, nil
}

func (g *groupResourceType) Grants(ctx context.Context, resource *v2.Resource, pt *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	bag, offset, err := parsePageToken(pt.Token, &v2.ResourceId{ResourceType: resourceTypeGroup.Id})
	if err != nil {
		return nil, "", nil, err
	}

	groupMembers, err := g.client.GetGroupMembers(
		ctx,
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

	var rv []*v2.Grant
	for _, member := range groupMembers {
		user, err := g.client.GetUser(ctx, member.User.Value)

		// There could be unreachable users, but available in group members table
		if err != nil {
			continue
		}

		userCopy := user

		ur, err := userResource(ctx, userCopy)
		if err != nil {
			return nil, "", nil, err
		}

		rv = append(
			rv,
			grant.NewGrant(
				resource,
				groupMembership,
				ur.Id,
			),
		)
	}

	if len(groupMembers) < ResourcesPageSize {
		return rv, "", nil, nil
	}

	return rv, nextPage, nil, nil
}

func groupBuilder(client *servicenow.Client) *groupResourceType {
	return &groupResourceType{
		resourceType: resourceTypeGroup,
		client:       client,
	}
}
