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

type rosterResourceType struct {
	resourceType *v2.ResourceType
	client       *servicenow.Client
}

func (r *rosterResourceType) ResourceType(_ context.Context) *v2.ResourceType {
	return r.resourceType
}

const rosterMembership = "member"

// Create a new connector resource for a ServiceNow on-call roster.
func rosterResource(roster *servicenow.Roster) (*v2.Resource, error) {
	profile := map[string]interface{}{
		"roster_name": roster.Name,
		"roster_id":   roster.Id,
		"rota_id":     roster.Rota,
	}

	groupTraitOptions := []rs.GroupTraitOption{
		rs.WithGroupProfile(profile),
	}

	displayName := roster.Name
	if displayName == "" {
		displayName = roster.Id
	}

	resource, err := rs.NewGroupResource(
		displayName,
		resourceTypeRoster,
		roster.Id,
		groupTraitOptions,
	)
	if err != nil {
		return nil, err
	}

	return resource, nil
}

func (r *rosterResourceType) List(ctx context.Context, _ *v2.ResourceId, pt *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	bag, offset, err := parsePageToken(pt.Token, &v2.ResourceId{ResourceType: resourceTypeRoster.Id})
	if err != nil {
		return nil, "", nil, err
	}

	rosters, nextPageToken, err := r.client.GetRosters(
		ctx,
		servicenow.PaginationVars{
			Limit:  ResourcesPageSize,
			Offset: offset,
		},
	)
	if err != nil {
		return nil, "", nil, fmt.Errorf("baton-servicenow: failed to list rosters: %w", err)
	}

	nextPage, err := bag.NextToken(nextPageToken)
	if err != nil {
		return nil, "", nil, err
	}

	var rv []*v2.Resource
	for _, roster := range rosters {
		rosterCopy := roster
		rr, err := rosterResource(&rosterCopy)
		if err != nil {
			return nil, "", nil, err
		}
		rv = append(rv, rr)
	}

	return rv, nextPage, nil, nil
}

func (r *rosterResourceType) Entitlements(ctx context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	var rv []*v2.Entitlement

	assignmentOptions := []ent.EntitlementOption{
		ent.WithGrantableTo(resourceTypeUser),
		ent.WithDisplayName(fmt.Sprintf("%s Roster %s", resource.DisplayName, rosterMembership)),
		ent.WithDescription(fmt.Sprintf("On-call membership in %s roster in ServiceNow", resource.DisplayName)),
	}

	rv = append(rv, ent.NewAssignmentEntitlement(
		resource,
		rosterMembership,
		assignmentOptions...,
	))

	return rv, "", nil, nil
}

func (r *rosterResourceType) Grants(ctx context.Context, resource *v2.Resource, pt *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	bag, offset, err := parsePageToken(pt.Token, &v2.ResourceId{ResourceType: resourceTypeRoster.Id})
	if err != nil {
		return nil, "", nil, err
	}

	rotaMembers, nextPageToken, err := r.client.GetRotaMembers(
		ctx,
		resource.Id.Resource, // all members of this roster
		"",
		servicenow.PaginationVars{
			Limit:  ResourcesPageSize,
			Offset: offset,
		},
	)
	if err != nil {
		return nil, "", nil, fmt.Errorf("baton-servicenow: failed to list roster members: %w", err)
	}

	nextPage, err := bag.NextToken(nextPageToken)
	if err != nil {
		return nil, "", nil, err
	}

	memberIDs := mapRotaMembers(rotaMembers)
	if len(memberIDs) == 0 {
		return []*v2.Grant{}, nextPage, nil, nil
	}

	var rv []*v2.Grant
	for _, member := range memberIDs {
		rID, err := rs.NewResourceID(resourceTypeUser, member)
		if err != nil {
			return nil, "", nil, fmt.Errorf("baton-servicenow: error creating principal id: %w", err)
		}

		rv = append(rv, grant.NewGrant(resource, rosterMembership, rID))
	}

	return rv, nextPage, nil, nil
}

func (r *rosterResourceType) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	if principal.Id.ResourceType != resourceTypeUser.Id {
		l.Warn(
			"baton-servicenow: only users can be added to an on-call roster",
			zap.String("principal_type", principal.Id.ResourceType),
			zap.String("principal_id", principal.Id.Resource),
		)
		return nil, nil
	}

	rosterId := entitlement.Resource.Id.Resource
	existing, _, err := r.client.GetRotaMembers(
		ctx,
		rosterId,
		principal.Id.Resource,
		servicenow.PaginationVars{Limit: 1},
	)
	if err != nil {
		return nil, fmt.Errorf("baton-servicenow: failed to get roster members for %s: %w", entitlement.Id, err)
	}

	// check if user is already on the roster
	if len(existing) > 0 {
		l.Warn(
			"baton-servicenow: cannot add user who is already on the roster",
			zap.String("roster", entitlement.Id),
			zap.String("user", principal.Id.Resource),
		)
		return annotations.New(&v2.GrantAlreadyExists{}), nil
	}

	err = r.client.AddUserToRoster(
		ctx,
		servicenow.RotaMemberPayload{
			Roster: rosterId,
			Member: principal.Id.Resource,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("baton-servicenow: failed to add user %s to roster %s: %w", principal.Id.Resource, rosterId, err)
	}

	return nil, nil
}

func (r *rosterResourceType) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	entitlement := grant.Entitlement
	principal := grant.Principal

	if principal.Id.ResourceType != resourceTypeUser.Id {
		l.Warn(
			"baton-servicenow: only users can be removed from an on-call roster",
			zap.String("principal_type", principal.Id.ResourceType),
			zap.String("principal_id", principal.Id.Resource),
		)

		return nil, nil
	}

	rosterId := entitlement.Resource.Id.Resource
	rotaMembers, _, err := r.client.GetRotaMembers(
		ctx,
		rosterId,
		principal.Id.Resource,
		servicenow.PaginationVars{Limit: 1},
	)
	if err != nil {
		return nil, fmt.Errorf("baton-servicenow: failed to get roster members for %s: %w", grant.Principal.Id.Resource, err)
	}

	// check if user is on the roster at all
	if len(rotaMembers) == 0 {
		l.Warn(
			"baton-servicenow: cannot remove user from a roster they are not on",
			zap.String("roster", entitlement.Id),
			zap.String("user", principal.Id.Resource),
		)
		return annotations.New(&v2.GrantAlreadyRevoked{}), nil
	}

	for _, member := range rotaMembers {
		err = r.client.RemoveRotaMember(ctx, member.Id)
		if err != nil {
			return nil, fmt.Errorf("baton-servicenow: failed to remove user %s from roster: %w", grant.Principal.Id.Resource, err)
		}
		l.Debug("revoked roster membership from user", zap.String("roster", grant.Entitlement.Id))
	}

	return nil, nil
}

func rosterBuilder(client *servicenow.Client) *rosterResourceType {
	return &rosterResourceType{
		resourceType: resourceTypeRoster,
		client:       client,
	}
}
