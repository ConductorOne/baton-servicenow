package connector

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"sync"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-servicenow/pkg/servicenow"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// servicenowEventFeedID identifies the connector's near-real-time event feed.
const servicenowEventFeedID = "servicenow_audit_feed"

var (
	_ connectorbuilder.EventProviderV2 = (*ServiceNow)(nil)
	_ connectorbuilder.EventFeed       = (*serviceNowEventFeed)(nil)
)

// snDatetimeLayout is the ServiceNow Table API datetime literal format (UTC).
const snDatetimeLayout = "2006-01-02 15:04:05"

// eventPageSize is the default rows-per-page when the SDK gives no page size.
const eventPageSize = 100

// EventFeeds returns the connector's event feeds.
func (s *ServiceNow) EventFeeds(ctx context.Context) []connectorbuilder.EventFeed {
	return []connectorbuilder.EventFeed{
		newServiceNowEventFeed(s.client),
	}
}

// auditDeleteFetcher fetches sys_audit_delete rows (revoke phase); a field so
// tests can inject a hard error to exercise phase isolation.
type auditDeleteFetcher func(ctx context.Context, tableNames []string, createdSince string, pv servicenow.PaginationVars) ([]servicenow.AuditDeleteRecord, string, error)

// auditFetcher fetches sys_audit rows (account-change phase); injectable in tests.
type auditFetcher func(ctx context.Context, tableNames []string, createdSince string, pv servicenow.PaginationVars) ([]servicenow.AuditRecord, string, error)

// Grant-create / account-change phase fetchers (live tables, no audit dependency).
type (
	groupMembersFetcher func(ctx context.Context, createdSince string, pv servicenow.PaginationVars) ([]servicenow.GroupMember, string, error)
	userRolesFetcher    func(ctx context.Context, createdSince string, pv servicenow.PaginationVars) ([]servicenow.UserToRole, string, error)
	groupRolesFetcher   func(ctx context.Context, createdSince string, pv servicenow.PaginationVars) ([]servicenow.GroupToRole, string, error)
	usersChangedFetcher func(ctx context.Context, changedSince string, pv servicenow.PaginationVars) ([]servicenow.User, string, error)
)

type serviceNowEventFeed struct {
	client *servicenow.Client

	fetchGroupMembers groupMembersFetcher
	fetchUserRoles    userRolesFetcher
	fetchGroupRoles   groupRolesFetcher
	fetchUsersChanged usersChangedFetcher
	fetchJoinDeletes  auditDeleteFetcher
	fetchAudit        auditFetcher

	fetchAuditDeletedTables func(ctx context.Context) ([]string, error)

	preflightOnce sync.Once
}

func newServiceNowEventFeed(client *servicenow.Client) *serviceNowEventFeed {
	f := &serviceNowEventFeed{client: client}
	f.fetchGroupMembers = client.GetGroupMembersCreatedSince
	f.fetchUserRoles = client.GetUserRolesCreatedSince
	f.fetchGroupRoles = client.GetGroupRolesCreatedSince
	f.fetchUsersChanged = func(ctx context.Context, changedSince string, pv servicenow.PaginationVars) ([]servicenow.User, string, error) {
		return client.GetUsersUpdatedSince(ctx, pv, changedSince)
	}
	f.fetchJoinDeletes = func(ctx context.Context, tableNames []string, createdSince string, pv servicenow.PaginationVars) ([]servicenow.AuditDeleteRecord, string, error) {
		return client.GetDeletedSincePayload(ctx, tableNames, createdSince, pv)
	}
	f.fetchAudit = func(ctx context.Context, tableNames []string, createdSince string, pv servicenow.PaginationVars) ([]servicenow.AuditRecord, string, error) {
		return client.GetAuditSince(ctx, tableNames, createdSince, pv)
	}
	f.fetchAuditDeletedTables = client.GetAuditDeletedTables
	return f
}

// revokeDetectionTables are the grant join tables whose deletes must be captured
// to sys_audit_delete (signalled by glide.ui.audit_deleted_tables) for revokes.
var revokeDetectionTables = []string{
	servicenow.TableUserGroupMember,
	servicenow.TableUserHasRole,
	servicenow.TableGroupHasRole,
}

// auditConfigPreflight runs the once-per-process advisory checks; never fatal.
func (f *serviceNowEventFeed) auditConfigPreflight(ctx context.Context) {
	f.preflightOnce.Do(func() {
		f.revokeDetectionPreflight(ctx)
	})
}

// revokeDetectionPreflight warns if any grant join table is absent from
// glide.ui.audit_deleted_tables, meaning its deletes (revokes) won't be captured.
func (f *serviceNowEventFeed) revokeDetectionPreflight(ctx context.Context) {
	l := ctxzap.Extract(ctx)

	deletedTables, err := f.fetchAuditDeletedTables(ctx)
	if err != nil {
		l.Warn("baton-servicenow: event-feed revoke-detection preflight could not verify revoke capture (sys_properties unreadable); revoke events may be silently missing",
			zap.Error(err),
		)
		return
	}

	captured := make(map[string]bool, len(deletedTables))
	for _, t := range deletedTables {
		captured[t] = true
	}

	var uncaptured []string
	for _, t := range revokeDetectionTables {
		if !captured[t] {
			uncaptured = append(uncaptured, t)
		}
	}
	if len(uncaptured) == 0 {
		l.Debug("baton-servicenow: event-feed revoke-detection preflight OK (grant join tables captured to sys_audit_delete)",
			zap.Strings("tables", revokeDetectionTables),
		)
		return
	}

	l.Warn("baton-servicenow: ServiceNow delete-capture is NOT enabled for grant join table(s); "+
		"near-real-time grant-REVOKE events are likely OFF for these — full-sync reconciliation "+
		"still covers removals. Remediation: add the table(s) to the glide.ui.audit_deleted_tables "+
		"system property so their deletes are recorded in sys_audit_delete.",
		zap.Strings("uncaptured_tables", uncaptured),
	)
}

func (f *serviceNowEventFeed) EventFeedMetadata(ctx context.Context) *v2.EventFeedMetadata {
	return &v2.EventFeedMetadata{
		Id: servicenowEventFeedID,
		SupportedEventTypes: []v2.EventType{
			v2.EventType_EVENT_TYPE_CREATE_GRANT,
			v2.EventType_EVENT_TYPE_CREATE_REVOKE,
			v2.EventType_EVENT_TYPE_RESOURCE_CHANGE,
		},
	}
}

// feedPhase selects a source table. Phases run in a fixed order; the cursor
// drains one fully (by offset) before advancing, so it encodes (phase, offset).
type feedPhase int

const (
	phaseGroupMembers feedPhase = iota // sys_user_grmember -> group "member" grants
	phaseUserRoles                     // sys_user_has_role  -> role "member" grants
	phaseGroupRoles                    // sys_group_has_role -> role "member" grants
	phaseUserChanges                   // sys_user (by sys_updated_on) -> account create/disable/modify
	phaseJoinDeletes                   // sys_audit_delete   -> grant "revoke" events
	phaseAudit                         // sys_audit          -> account hard-delete
	phaseDone
)

// isAuditBackedPhase reports phases reading the audit tables. A hard error there
// (e.g. ACL denial) is skipped not fatal; grant-create phases instead propagate.
func isAuditBackedPhase(p feedPhase) bool {
	return p == phaseJoinDeletes || p == phaseAudit
}

// feedCursor is the JSON-encoded StreamToken cursor for the feed.
type feedCursor struct {
	Phase  feedPhase `json:"phase"`
	Offset int       `json:"offset"`
}

func parseFeedCursor(s string) (feedCursor, error) {
	if s == "" {
		return feedCursor{Phase: phaseGroupMembers, Offset: 0}, nil
	}
	var c feedCursor
	if err := json.Unmarshal([]byte(s), &c); err != nil {
		return feedCursor{}, fmt.Errorf("baton-servicenow: invalid event-feed cursor: %w", err)
	}
	return c, nil
}

func (c feedCursor) encode() (string, error) {
	b, err := json.Marshal(c)
	if err != nil {
		return "", fmt.Errorf("baton-servicenow: failed to encode event-feed cursor: %w", err)
	}
	return string(b), nil
}

// ListEvents returns the next page of events at or after earliestEvent, walking
// the source tables phase by phase via the (phase, offset) StreamToken cursor.
func (f *serviceNowEventFeed) ListEvents(
	ctx context.Context,
	earliestEvent *timestamppb.Timestamp,
	pToken *pagination.StreamToken,
) ([]*v2.Event, *pagination.StreamState, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	f.auditConfigPreflight(ctx)

	cursor, err := parseFeedCursor(pToken.Cursor)
	if err != nil {
		return nil, nil, nil, err
	}

	since := ""
	if earliestEvent != nil {
		since = earliestEvent.AsTime().UTC().Format(snDatetimeLayout)
	}

	pageSize := pToken.Size
	if pageSize <= 0 {
		pageSize = eventPageSize
	}
	pv := servicenow.PaginationVars{Limit: pageSize, Offset: cursor.Offset}

	for cursor.Phase < phaseDone {
		events, next, err := f.fetchPhase(ctx, cursor.Phase, since, pv)
		if err != nil {
			// Phase isolation: a hard error on an audit-backed phase is logged and
			// skipped (full sync reconciles); grant-create phase errors propagate.
			if isAuditBackedPhase(cursor.Phase) {
				l.Warn("baton-servicenow: skipping audit-backed event-feed phase after fetch error (full sync will reconcile)",
					zap.Int("phase", int(cursor.Phase)),
					zap.Error(err),
				)
				cursor.Phase++
				cursor.Offset = 0
				pv.Offset = 0
				continue
			}
			return nil, nil, nil, err
		}

		// More rows in this phase: stay on the phase, advance the offset.
		if next != "" {
			off, convErr := servicenow.ConvertPageToken(next)
			if convErr != nil {
				return nil, nil, nil, fmt.Errorf("baton-servicenow: failed to parse event-feed page token: %w", convErr)
			}
			// Guard against a non-advancing token (treat the phase as drained).
			if off > cursor.Offset {
				cursor.Offset = off
				state, encErr := streamState(cursor, true)
				if encErr != nil {
					return nil, nil, nil, encErr
				}
				return events, state, nil, nil
			}
		}

		// Phase drained: advance to the next phase, reset offset.
		cursor.Phase++
		cursor.Offset = 0
		pv.Offset = 0

		if len(events) > 0 {
			state, encErr := streamState(cursor, cursor.Phase < phaseDone)
			if encErr != nil {
				return nil, nil, nil, encErr
			}
			return events, state, nil, nil
		}
		l.Debug("baton-servicenow: event-feed phase produced no events, advancing", zap.Int("phase", int(cursor.Phase-1)))
	}

	state, err := streamState(feedCursor{Phase: phaseDone}, false)
	if err != nil {
		return nil, nil, nil, err
	}
	return []*v2.Event{}, state, nil, nil
}

func streamState(cursor feedCursor, hasMore bool) (*pagination.StreamState, error) {
	enc, err := cursor.encode()
	if err != nil {
		return nil, err
	}
	return &pagination.StreamState{Cursor: enc, HasMore: hasMore}, nil
}

// fetchPhase fetches one page from the given phase's source and maps it to events.
func (f *serviceNowEventFeed) fetchPhase(
	ctx context.Context,
	phase feedPhase,
	since string,
	pv servicenow.PaginationVars,
) ([]*v2.Event, string, error) {
	switch phase {
	case phaseGroupMembers:
		rows, next, err := f.fetchGroupMembers(ctx, since, pv)
		if err != nil {
			return nil, "", fmt.Errorf("baton-servicenow: failed to list new group memberships: %w", err)
		}
		evs := make([]*v2.Event, 0, len(rows))
		for i := range rows {
			evs = append(evs, groupMemberCreateGrantEvent(&rows[i]))
		}
		return evs, next, nil

	case phaseUserRoles:
		rows, next, err := f.fetchUserRoles(ctx, since, pv)
		if err != nil {
			return nil, "", fmt.Errorf("baton-servicenow: failed to list new user roles: %w", err)
		}
		evs := make([]*v2.Event, 0, len(rows))
		for i := range rows {
			evs = append(evs, userRoleCreateGrantEvent(&rows[i]))
		}
		return evs, next, nil

	case phaseGroupRoles:
		rows, next, err := f.fetchGroupRoles(ctx, since, pv)
		if err != nil {
			return nil, "", fmt.Errorf("baton-servicenow: failed to list new group roles: %w", err)
		}
		evs := make([]*v2.Event, 0, len(rows))
		for i := range rows {
			evs = append(evs, groupRoleCreateGrantEvent(&rows[i]))
		}
		return evs, next, nil

	case phaseUserChanges:
		rows, next, err := f.fetchUsersChanged(ctx, since, pv)
		if err != nil {
			return nil, "", fmt.Errorf("baton-servicenow: failed to list changed users: %w", err)
		}
		evs := make([]*v2.Event, 0, len(rows))
		for i := range rows {
			evs = append(evs, userChangeEvent(&rows[i]))
		}
		return evs, next, nil

	case phaseJoinDeletes:
		rows, next, err := f.fetchJoinDeletes(ctx, joinDeleteTables, since, pv)
		if err != nil {
			return nil, "", fmt.Errorf("baton-servicenow: failed to list deleted grants: %w", err)
		}
		evs := make([]*v2.Event, 0, len(rows))
		skips := map[string]int{}
		for i := range rows {
			ev, skipReason := joinDeleteRevokeEvent(&rows[i])
			if ev != nil {
				evs = append(evs, ev)
			} else if skipReason != "" {
				skips[skipReason]++
			}
		}
		logSkippedDeletes(ctx, skips)
		return evs, next, nil

	case phaseAudit:
		rows, next, err := f.fetchAudit(ctx, auditChangeTables, since, pv)
		if err != nil {
			return nil, "", fmt.Errorf("baton-servicenow: failed to list audit changes: %w", err)
		}
		evs := make([]*v2.Event, 0, len(rows))
		for i := range rows {
			if ev := auditChangeEvent(&rows[i]); ev != nil {
				evs = append(evs, ev)
			}
		}
		return evs, next, nil

	default:
		return nil, "", nil
	}
}

// occurredAt parses a ServiceNow datetime literal, falling back to now.
func occurredAt(snDatetime string) *timestamppb.Timestamp {
	if t, err := time.ParseInLocation(snDatetimeLayout, snDatetime, time.UTC); err == nil {
		return timestamppb.New(t)
	}
	return timestamppb.Now()
}

// groupMemberCreateGrantEvent: new sys_user_grmember -> user granted group "member".
func groupMemberCreateGrantEvent(m *servicenow.GroupMember) *v2.Event {
	groupRes := minimalResource(resourceTypeGroup, m.Group)
	principal := minimalResource(resourceTypeUser, m.User)
	return &v2.Event{
		Id:         "grmember:" + m.Id,
		OccurredAt: occurredAt(m.SysCreatedOn),
		Event: &v2.Event_CreateGrantEvent{
			CreateGrantEvent: &v2.CreateGrantEvent{
				Entitlement: ent.NewAssignmentEntitlement(groupRes, groupMembership),
				Principal:   principal,
			},
		},
	}
}

// userRoleCreateGrantEvent: new sys_user_has_role -> user granted role "member".
func userRoleCreateGrantEvent(r *servicenow.UserToRole) *v2.Event {
	roleRes := minimalResource(resourceTypeRole, r.Role)
	principal := minimalResource(resourceTypeUser, r.User)
	return &v2.Event{
		Id:         "userrole:" + r.Id,
		OccurredAt: occurredAt(r.SysCreatedOn),
		Event: &v2.Event_CreateGrantEvent{
			CreateGrantEvent: &v2.CreateGrantEvent{
				Entitlement: ent.NewAssignmentEntitlement(roleRes, roleMembership),
				Principal:   principal,
			},
		},
	}
}

// groupRoleCreateGrantEvent: new sys_group_has_role -> group granted role "member".
func groupRoleCreateGrantEvent(r *servicenow.GroupToRole) *v2.Event {
	roleRes := minimalResource(resourceTypeRole, r.Role)
	principal := minimalResource(resourceTypeGroup, r.Group)
	return &v2.Event{
		Id:         "grouprole:" + r.Id,
		OccurredAt: occurredAt(r.SysCreatedOn),
		Event: &v2.Event_CreateGrantEvent{
			CreateGrantEvent: &v2.CreateGrantEvent{
				Entitlement: ent.NewAssignmentEntitlement(roleRes, roleMembership),
				Principal:   principal,
			},
		},
	}
}

// joinDeleteTables are the grant join tables whose hard deletes become revokes.
var joinDeleteTables = []string{
	servicenow.TableUserGroupMember,
	servicenow.TableUserHasRole,
	servicenow.TableGroupHasRole,
}

// auditChangeTables scopes the sys_audit phase to sys_user. Only account
// hard-deletes map to an event (auditChangeEvent); audit rows for the other
// tables would be paged through and discarded, so we never fetch them.
var auditChangeTables = []string{servicenow.TableUser}

// deletedRowPayload holds the reference fields parsed from a
// sys_audit_delete.payload XML dump of a deleted join row.
type deletedRowPayload struct {
	Group string `xml:"group"`
	User  string `xml:"user"`
	Role  string `xml:"role"`
}

func parseDeletedRowPayload(payload string) (*deletedRowPayload, error) {
	var p deletedRowPayload
	if err := xml.Unmarshal([]byte(payload), &p); err != nil {
		return nil, err
	}
	return &p, nil
}

// joinDeleteRevokeEvent maps a sys_audit_delete row to a grant-revoke event,
// resolving principal+entitlement from the deleted row's XML payload (the bare
// sys_audit row can't). Returns (nil, reason) for any row it cannot map; the
// caller aggregates the reasons so a drain full of unmappable rows logs a few
// summary lines instead of one warning per row. A reason of "" means the row
// was for an unrelated table and is silently ignored.
func joinDeleteRevokeEvent(d *servicenow.AuditDeleteRecord) (*v2.Event, string) {
	if d.Payload == "" {
		return nil, "empty payload (delete-recovery may be disabled)"
	}

	p, err := parseDeletedRowPayload(d.Payload)
	if err != nil {
		return nil, "unparseable payload"
	}

	var principal *v2.Resource
	var entitlement *v2.Entitlement

	switch d.Tablename {
	case servicenow.TableUserGroupMember:
		if p.User == "" || p.Group == "" {
			break
		}
		principal = minimalResource(resourceTypeUser, p.User)
		entitlement = ent.NewAssignmentEntitlement(minimalResource(resourceTypeGroup, p.Group), groupMembership)
	case servicenow.TableUserHasRole:
		if p.User == "" || p.Role == "" {
			break
		}
		principal = minimalResource(resourceTypeUser, p.User)
		entitlement = ent.NewAssignmentEntitlement(minimalResource(resourceTypeRole, p.Role), roleMembership)
	case servicenow.TableGroupHasRole:
		if p.Group == "" || p.Role == "" {
			break
		}
		principal = minimalResource(resourceTypeGroup, p.Group)
		entitlement = ent.NewAssignmentEntitlement(minimalResource(resourceTypeRole, p.Role), roleMembership)
	default:
		return nil, ""
	}

	if principal == nil || entitlement == nil {
		return nil, "incomplete payload references"
	}

	return &v2.Event{
		Id:         "revoke:" + d.DocumentKey,
		OccurredAt: occurredAt(d.SysCreatedOn),
		Event: &v2.Event_CreateRevokeEvent{
			CreateRevokeEvent: &v2.CreateRevokeEvent{
				Entitlement: entitlement,
				Principal:   principal,
			},
		},
	}, ""
}

// logSkippedDeletes emits one Warn per distinct skip reason for a page of
// sys_audit_delete rows, with the count, so a drain where delete-recovery is
// off (every row has an empty payload) logs a handful of lines instead of one
// per row.
func logSkippedDeletes(ctx context.Context, skips map[string]int) {
	if len(skips) == 0 {
		return
	}
	l := ctxzap.Extract(ctx)
	for reason, n := range skips {
		l.Warn("baton-servicenow: skipped unmappable deleted-grant rows in event feed",
			zap.String("reason", reason),
			zap.Int("count", n),
		)
	}
}

// auditChangeEvent handles the account hard-DELETE path: a deleted sys_user
// writes a sys_audit "DELETED" row regardless of the audit flag, emitting a
// RESOURCE_CHANGE so ConductorOne re-syncs (and drops) the account. Join-table
// rows are skipped (their documentkey is only the join-row sys_id).
func auditChangeEvent(a *servicenow.AuditRecord) *v2.Event {
	if a.Tablename != servicenow.TableUser {
		return nil
	}
	return &v2.Event{
		Id:         "audit:" + a.SysID,
		OccurredAt: occurredAt(a.SysCreatedOn),
		Event: &v2.Event_ResourceChangeEvent{
			ResourceChangeEvent: &v2.ResourceChangeEvent{
				ResourceId: &v2.ResourceId{
					ResourceType: resourceTypeUser.Id,
					Resource:     a.DocumentKey,
				},
			},
		},
	}
}

// userChangeEvent is the audit-INDEPENDENT account source: polling sys_user by
// sys_updated_on catches creates and field changes (notably active=false
// disables) that sys_audit misses (no insert auditing; sys_user auditing off by
// default). Hard-deletes are handled by phaseAudit instead.
func userChangeEvent(u *servicenow.User) *v2.Event {
	return &v2.Event{
		Id:         "userchange:" + u.Id,
		OccurredAt: occurredAt(u.SysUpdatedOn),
		Event: &v2.Event_ResourceChangeEvent{
			ResourceChangeEvent: &v2.ResourceChangeEvent{
				ResourceId: &v2.ResourceId{
					ResourceType: resourceTypeUser.Id,
					Resource:     u.Id,
				},
			},
		},
	}
}

// minimalResource builds an id/type-only resource, sufficient for event payloads.
func minimalResource(rt *v2.ResourceType, id string) *v2.Resource {
	r, err := rs.NewResource(id, rt, id)
	if err != nil {
		return &v2.Resource{Id: &v2.ResourceId{ResourceType: rt.Id, Resource: id}}
	}
	return r
}
