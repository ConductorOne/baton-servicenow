package connector

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

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

	// moduleAbsentOnce guards a single advisory warning when the On-Call
	// Scheduling plugin is not installed (see warnModuleAbsent).
	moduleAbsentOnce sync.Once
}

// warnModuleAbsent logs, at most once per process, that the On-Call Scheduling
// plugin is not installed, so schedule resources are being skipped. It is called
// when a schedule read hits a ServiceNow "Invalid table" error on the on-call
// tables (cmn_rota_roster / cmn_rota_member), which means the optional plugin
// (com.snc.on_call_rotation) is absent. Skipping schedules must NOT fail the
// sync of users, groups, and roles (separate resource types), so callers return
// empty rather than propagating the error.
func (s *scheduleResourceType) warnModuleAbsent(ctx context.Context) {
	s.moduleAbsentOnce.Do(func() {
		ctxzap.Extract(ctx).Warn(
			"baton-servicenow: ServiceNow On-Call Scheduling plugin (com.snc.on_call_rotation) " +
				"appears not to be installed (cmn_rota_roster table is absent); skipping schedule " +
				"resources, entitlements, and grants. User, group, and role sync are unaffected. " +
				"Install the On-Call Scheduling plugin to enable schedule support.",
		)
	})
}

func (s *scheduleResourceType) ResourceType(_ context.Context) *v2.ResourceType {
	return s.resourceType
}

const (
	scheduleMember  = "member"  // anyone on the schedule's roster (provisionable)
	scheduleOnCall  = "on-call" // the user currently on call (whoisoncall order==1); read-only
	scheduleManager = "manager" // the assignment group's manager (sys_user_group.manager); read-only
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
		// On-Call Scheduling plugin not installed: skip schedules instead of
		// failing the whole sync (users/groups/roles are independent).
		if servicenow.IsInvalidTableError(err) {
			s.warnModuleAbsent(ctx)
			return nil, "", nil, nil
		}
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

	// on-call and manager are derived (current rotation / group manager), not
	// granted through this connector — mark them immutable (read-only in C1).
	onCallOptions := []ent.EntitlementOption{
		ent.WithGrantableTo(resourceTypeUser),
		ent.WithDisplayName(fmt.Sprintf("%s schedule %s", resource.DisplayName, scheduleOnCall)),
		ent.WithDescription(fmt.Sprintf("%s ServiceNow schedule %s", resource.DisplayName, scheduleOnCall)),
		ent.WithAnnotation(&v2.EntitlementImmutable{}),
	}

	managerOptions := []ent.EntitlementOption{
		ent.WithGrantableTo(resourceTypeUser),
		ent.WithDisplayName(fmt.Sprintf("%s schedule %s", resource.DisplayName, scheduleManager)),
		ent.WithDescription(fmt.Sprintf("%s ServiceNow schedule %s (assignment group manager)", resource.DisplayName, scheduleManager)),
		ent.WithAnnotation(&v2.EntitlementImmutable{}),
	}

	rv = append(rv,
		ent.NewAssignmentEntitlement(resource, scheduleMember, memberOptions...),
		ent.NewAssignmentEntitlement(resource, scheduleOnCall, onCallOptions...),
		ent.NewAssignmentEntitlement(resource, scheduleManager, managerOptions...),
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
		// Defensive: if the on-call plugin was uninstalled after List ran, skip
		// rather than fail the sync.
		if servicenow.IsInvalidTableError(err) {
			s.warnModuleAbsent(ctx)
			return nil, "", nil, nil
		}
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

		// manager = the assignment group's manager (schedule -> rota -> group).
		managerID, err := s.scheduleManagerUserID(ctx, resource.Id.Resource)
		if err != nil {
			return nil, "", nil, err
		}
		if managerID != "" {
			rID, err := rs.NewResourceID(resourceTypeUser, managerID)
			if err != nil {
				return nil, "", nil, fmt.Errorf("baton-servicenow: error creating manager principal id: %w", err)
			}
			rv = append(rv, grant.NewGrant(resource, scheduleManager, rID))
		}
	}

	return rv, nextPage, nil, nil
}

// scheduleManagerUserID resolves the schedule's manager (the assignment group's
// manager) via roster -> rota -> group. Returns "" if no manager is set.
func (s *scheduleResourceType) scheduleManagerUserID(ctx context.Context, rosterId string) (string, error) {
	roster, err := s.client.GetRoster(ctx, rosterId)
	if err != nil {
		return "", fmt.Errorf("baton-servicenow: failed to get roster %s: %w", rosterId, err)
	}
	if roster.Rota == "" {
		return "", nil
	}
	rota, err := s.client.GetRota(ctx, roster.Rota)
	if err != nil {
		return "", fmt.Errorf("baton-servicenow: failed to get rota %s: %w", roster.Rota, err)
	}
	if rota.Group == "" {
		return "", nil
	}
	group, err := s.client.GetGroup(ctx, rota.Group)
	if err != nil {
		return "", fmt.Errorf("baton-servicenow: failed to get group %s: %w", rota.Group, err)
	}
	return group.Manager, nil
}

// Grant adds a user to a schedule's roster via the on_call_add_member action
// table (the supported path that the engine processes server-side). Only the
// "member" entitlement is provisionable; on-call and manager are read-only.
// Because the on-call engine only includes roster members who also belong to
// the assignment group, the user is added to that group first if needed.
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

	if entitlementSlug(entitlement) != scheduleMember {
		l.Warn(
			"baton-servicenow: only the schedule member entitlement is provisionable (on-call and manager are read-only)",
			zap.String("entitlement", entitlement.Id),
		)
		return nil, nil
	}

	rosterId := entitlement.Resource.Id.Resource

	// idempotency: already on the roster?
	existing, _, err := s.client.GetRotaMembers(ctx, rosterId, principal.Id.Resource, servicenow.PaginationVars{Limit: 1})
	if err != nil {
		return nil, fmt.Errorf("baton-servicenow: failed to get schedule members for %s: %w", entitlement.Id, err)
	}
	if len(existing) > 0 {
		return annotations.New(&v2.GrantAlreadyExists{}), nil
	}

	// resolve the rota + assignment group for this roster
	roster, err := s.client.GetRoster(ctx, rosterId)
	if err != nil {
		return nil, fmt.Errorf("baton-servicenow: failed to get roster %s: %w", rosterId, err)
	}
	groupId := ""
	if roster.Rota != "" {
		rota, err := s.client.GetRota(ctx, roster.Rota)
		if err != nil {
			return nil, fmt.Errorf("baton-servicenow: failed to get rota %s: %w", roster.Rota, err)
		}
		groupId = rota.Group
	}

	// the on-call engine only includes members who are in the assignment group
	if groupId != "" {
		inGroup, _, err := s.client.GetUserToGroup(ctx, principal.Id.Resource, groupId, servicenow.PaginationVars{Limit: 1})
		if err != nil {
			return nil, fmt.Errorf("baton-servicenow: failed to check assignment group membership: %w", err)
		}
		if len(inGroup) == 0 {
			if err := s.client.AddUserToGroup(ctx, servicenow.GroupMemberPayload{User: principal.Id.Resource, Group: groupId}); err != nil {
				return nil, fmt.Errorf("baton-servicenow: failed to add user %s to assignment group %s: %w", principal.Id.Resource, groupId, err)
			}
			l.Info("baton-servicenow: added user to assignment group (prerequisite for on-call schedule membership)",
				zap.String("user", principal.Id.Resource), zap.String("group", groupId))
		}
	}

	err = s.client.AddOnCallMember(ctx, servicenow.OnCallAddMemberPayload{
		Member:   principal.Id.Resource,
		Rosters:  rosterId,
		Rota:     roster.Rota,
		FromDate: onCallActionDate(),
	})
	if err != nil {
		return nil, fmt.Errorf("baton-servicenow: failed to add user %s to schedule %s: %w", principal.Id.Resource, rosterId, err)
	}

	return nil, nil
}

// Revoke removes a user from a schedule's roster via the on_call_remove_member
// action table. Assignment group membership is intentionally left intact.
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

	if entitlementSlug(entitlement) != scheduleMember {
		l.Warn(
			"baton-servicenow: only the schedule member entitlement is provisionable (on-call and manager are read-only)",
			zap.String("entitlement", entitlement.Id),
		)
		return nil, nil
	}

	rosterId := entitlement.Resource.Id.Resource

	// idempotency: is the user on the roster at all?
	existing, _, err := s.client.GetRotaMembers(ctx, rosterId, principal.Id.Resource, servicenow.PaginationVars{Limit: 1})
	if err != nil {
		return nil, fmt.Errorf("baton-servicenow: failed to get schedule members for %s: %w", grant.Principal.Id.Resource, err)
	}
	if len(existing) == 0 {
		return annotations.New(&v2.GrantAlreadyRevoked{}), nil
	}

	roster, err := s.client.GetRoster(ctx, rosterId)
	if err != nil {
		return nil, fmt.Errorf("baton-servicenow: failed to get roster %s: %w", rosterId, err)
	}

	err = s.client.RemoveOnCallMember(ctx, servicenow.OnCallRemoveMemberPayload{
		User:         principal.Id.Resource,
		Rosters:      rosterId,
		Rota:         roster.Rota,
		FromDate:     onCallActionDate(),
		DeleteMember: "true",
	})
	if err != nil {
		return nil, fmt.Errorf("baton-servicenow: failed to remove user %s from schedule %s: %w", principal.Id.Resource, rosterId, err)
	}

	return nil, nil
}

// entitlementSlug returns the trailing slug of an entitlement id
// ("<type>:<resourceId>:<slug>"), falling back to the Slug field.
func entitlementSlug(e *v2.Entitlement) string {
	if i := strings.LastIndex(e.Id, ":"); i >= 0 && i < len(e.Id)-1 {
		return e.Id[i+1:]
	}
	return e.Slug
}

// onCallActionDate is the from_date for on-call add/remove actions: today (UTC).
func onCallActionDate() string {
	return time.Now().UTC().Format("2006-01-02")
}

func scheduleBuilder(client *servicenow.Client) *scheduleResourceType {
	return &scheduleResourceType{
		resourceType: resourceTypeSchedule,
		client:       client,
	}
}
