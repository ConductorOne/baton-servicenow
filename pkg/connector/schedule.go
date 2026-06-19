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

// The connector's "schedule" resource maps to ServiceNow's on-call roster
// (cmn_rota_roster) — the level at which on-call membership (cmn_rota_member)
// is defined. The "schedule" naming aligns with the other on-call connectors
// (PagerDuty, OpsGenie, Rootly); the ServiceNow API layer keeps the native
// roster/rota terminology that matches the underlying tables.
type scheduleResourceType struct {
	resourceType *v2.ResourceType
	client       *servicenow.Client
}

func (s *scheduleResourceType) ResourceType(_ context.Context) *v2.ResourceType {
	return s.resourceType
}

const (
	scheduleMember = "member"  // anyone on the schedule's roster
	scheduleOnCall = "on-call" // the user currently on call (whoisoncall order==1)
)

// Create a new connector resource for a ServiceNow on-call schedule (roster).
func scheduleResource(roster *servicenow.Roster) (*v2.Resource, error) {
	profile := map[string]interface{}{
		"schedule_name": roster.Name,
		"schedule_id":   roster.Id,
		"rota_id":       roster.Rota,
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
		resourceTypeSchedule,
		roster.Id,
		groupTraitOptions,
	)
	if err != nil {
		return nil, err
	}

	return resource, nil
}

func (s *scheduleResourceType) List(ctx context.Context, _ *v2.ResourceId, pt *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	bag, offset, err := parsePageToken(pt.Token, &v2.ResourceId{ResourceType: resourceTypeSchedule.Id})
	if err != nil {
		return nil, "", nil, err
	}

	rosters, nextPageToken, err := s.client.GetRosters(
		ctx,
		servicenow.PaginationVars{
			Limit:  ResourcesPageSize,
			Offset: offset,
		},
	)
	if err != nil {
		return nil, "", nil, fmt.Errorf("baton-servicenow: failed to list schedules: %w", err)
	}

	nextPage, err := bag.NextToken(nextPageToken)
	if err != nil {
		return nil, "", nil, err
	}

	var rv []*v2.Resource
	for _, roster := range rosters {
		rosterCopy := roster
		rr, err := scheduleResource(&rosterCopy)
		if err != nil {
			return nil, "", nil, err
		}
		rv = append(rv, rr)
	}

	return rv, nextPage, nil, nil
}

func (s *scheduleResourceType) Entitlements(ctx context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	var rv []*v2.Entitlement

	memberOptions := []ent.EntitlementOption{
		ent.WithGrantableTo(resourceTypeUser),
		ent.WithDisplayName(fmt.Sprintf("%s schedule %s", resource.DisplayName, scheduleMember)),
		ent.WithDescription(fmt.Sprintf("%s ServiceNow schedule %s", resource.DisplayName, scheduleMember)),
	}

	onCallOptions := []ent.EntitlementOption{
		ent.WithGrantableTo(resourceTypeUser),
		ent.WithDisplayName(fmt.Sprintf("%s schedule %s", resource.DisplayName, scheduleOnCall)),
		ent.WithDescription(fmt.Sprintf("%s ServiceNow schedule %s", resource.DisplayName, scheduleOnCall)),
	}

	rv = append(rv,
		ent.NewAssignmentEntitlement(resource, scheduleMember, memberOptions...),
		ent.NewAssignmentEntitlement(resource, scheduleOnCall, onCallOptions...),
	)

	return rv, "", nil, nil
}

func (s *scheduleResourceType) Grants(ctx context.Context, resource *v2.Resource, pt *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	bag, offset, err := parsePageToken(pt.Token, &v2.ResourceId{ResourceType: resourceTypeSchedule.Id})
	if err != nil {
		return nil, "", nil, err
	}

	rotaMembers, nextPageToken, err := s.client.GetRotaMembers(
		ctx,
		resource.Id.Resource, // all members of this schedule's roster
		"",
		servicenow.PaginationVars{
			Limit:  ResourcesPageSize,
			Offset: offset,
		},
	)
	if err != nil {
		return nil, "", nil, fmt.Errorf("baton-servicenow: failed to list schedule members: %w", err)
	}

	nextPage, err := bag.NextToken(nextPageToken)
	if err != nil {
		return nil, "", nil, err
	}

	var rv []*v2.Grant
	for _, member := range mapRotaMembers(rotaMembers) {
		rID, err := rs.NewResourceID(resourceTypeUser, member)
		if err != nil {
			return nil, "", nil, fmt.Errorf("baton-servicenow: error creating principal id: %w", err)
		}

		rv = append(rv, grant.NewGrant(resource, scheduleMember, rID))
	}

	// On the first page only, emit the on-call grant for whoever is currently
	// on call (whoisoncall Order==1). This is a single computation independent
	// of member pagination, so it must not be repeated on subsequent pages.
	if offset == 0 {
		onCall, err := s.client.WhoIsOnCall(ctx, resource.Id.Resource)
		if err != nil {
			return nil, "", nil, fmt.Errorf("baton-servicenow: failed to get on-call user for schedule %s: %w", resource.Id.Resource, err)
		}
		for _, oc := range onCall {
			if oc.Order != 1 || oc.UserId == "" {
				continue
			}
			rID, err := rs.NewResourceID(resourceTypeUser, oc.UserId)
			if err != nil {
				return nil, "", nil, fmt.Errorf("baton-servicenow: error creating on-call principal id: %w", err)
			}
			rv = append(rv, grant.NewGrant(resource, scheduleOnCall, rID))
		}
	}

	return rv, nextPage, nil, nil
}

func (s *scheduleResourceType) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	if principal.Id.ResourceType != resourceTypeUser.Id {
		l.Warn(
			"baton-servicenow: only users can be added to an on-call schedule",
			zap.String("principal_type", principal.Id.ResourceType),
			zap.String("principal_id", principal.Id.Resource),
		)
		return nil, nil
	}

	rosterId := entitlement.Resource.Id.Resource
	existing, _, err := s.client.GetRotaMembers(
		ctx,
		rosterId,
		principal.Id.Resource,
		servicenow.PaginationVars{Limit: 1},
	)
	if err != nil {
		return nil, fmt.Errorf("baton-servicenow: failed to get schedule members for %s: %w", entitlement.Id, err)
	}

	// check if user is already on the schedule's roster
	if len(existing) > 0 {
		l.Warn(
			"baton-servicenow: cannot add user who is already on the schedule",
			zap.String("schedule", entitlement.Id),
			zap.String("user", principal.Id.Resource),
		)
		return annotations.New(&v2.GrantAlreadyExists{}), nil
	}

	err = s.client.AddUserToRoster(
		ctx,
		servicenow.RotaMemberPayload{
			Roster: rosterId,
			Member: principal.Id.Resource,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("baton-servicenow: failed to add user %s to schedule %s: %w", principal.Id.Resource, rosterId, err)
	}

	return nil, nil
}

func (s *scheduleResourceType) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	entitlement := grant.Entitlement
	principal := grant.Principal

	if principal.Id.ResourceType != resourceTypeUser.Id {
		l.Warn(
			"baton-servicenow: only users can be removed from an on-call schedule",
			zap.String("principal_type", principal.Id.ResourceType),
			zap.String("principal_id", principal.Id.Resource),
		)

		return nil, nil
	}

	rosterId := entitlement.Resource.Id.Resource
	rotaMembers, _, err := s.client.GetRotaMembers(
		ctx,
		rosterId,
		principal.Id.Resource,
		servicenow.PaginationVars{Limit: 1},
	)
	if err != nil {
		return nil, fmt.Errorf("baton-servicenow: failed to get schedule members for %s: %w", grant.Principal.Id.Resource, err)
	}

	// check if user is on the schedule's roster at all
	if len(rotaMembers) == 0 {
		l.Warn(
			"baton-servicenow: cannot remove user from a schedule they are not on",
			zap.String("schedule", entitlement.Id),
			zap.String("user", principal.Id.Resource),
		)
		return annotations.New(&v2.GrantAlreadyRevoked{}), nil
	}

	for _, member := range rotaMembers {
		err = s.client.RemoveRotaMember(ctx, member.Id)
		if err != nil {
			return nil, fmt.Errorf("baton-servicenow: failed to remove user %s from schedule: %w", grant.Principal.Id.Resource, err)
		}
		l.Debug("revoked schedule membership from user", zap.String("schedule", grant.Entitlement.Id))
	}

	return nil, nil
}

func scheduleBuilder(client *servicenow.Client) *scheduleResourceType {
	return &scheduleResourceType{
		resourceType: resourceTypeSchedule,
		client:       client,
	}
}
