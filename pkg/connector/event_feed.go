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

// servicenowEventFeedID is the stable identifier for the connector's single
// near-real-time event feed (the analog of Okta's System Log). It polls several
// ServiceNow sources, preferring audit-INDEPENDENT ones so detection does not
// depend on instance audit configuration:
//   - the membership/assignment join tables for newly created rows (grant-created
//     events), and sys_audit_delete for their deletes (grant-revoke events);
//   - sys_user by sys_updated_on for account CREATES and field changes — notably
//     active=false DISABLES — emitted as RESOURCE_CHANGE; plus sys_audit for
//     account hard-DELETES (also RESOURCE_CHANGE).
const servicenowEventFeedID = "servicenow_audit_feed"

// Compile-time assertions that the connector implements the recommended
// EventProviderV2 interface and that the feed implements EventFeed.
var (
	_ connectorbuilder.EventProviderV2 = (*ServiceNow)(nil)
	_ connectorbuilder.EventFeed       = (*serviceNowEventFeed)(nil)
)

// snDatetimeLayout is the ServiceNow Table API datetime literal format
// ("YYYY-MM-DD HH:MM:SS", UTC) used in sysparm_query comparisons.
const snDatetimeLayout = "2006-01-02 15:04:05"

// eventPageSize bounds the number of audit/membership rows fetched per
// ListEvents call when the SDK does not specify a page size.
const eventPageSize = 100

// EventFeeds returns the connector's event feeds. Implementing this (via the
// EventProviderV2 interface) opts the connector into near-real-time change
// detection, independent of the periodic full sync.
func (s *ServiceNow) EventFeeds(ctx context.Context) []connectorbuilder.EventFeed {
	return []connectorbuilder.EventFeed{
		newServiceNowEventFeed(s.client),
	}
}

// auditDeleteFetcher fetches one page of sys_audit_delete rows (with payload)
// for the revoke phase. It mirrors Client.GetDeletedSincePayload; a field so
// tests can inject a hard error to exercise phase isolation.
type auditDeleteFetcher func(ctx context.Context, tableNames []string, createdSince string, pv servicenow.PaginationVars) ([]servicenow.AuditDeleteRecord, string, error)

// auditFetcher fetches one page of sys_audit rows for the account-change phase.
// It mirrors Client.GetAuditSince; a field so tests can inject a hard error.
type auditFetcher func(ctx context.Context, tableNames []string, createdSince string, pv servicenow.PaginationVars) ([]servicenow.AuditRecord, string, error)

// Grant-create phase fetchers (live join tables, no audit dependency). Fields so
// tests can drive ListEvents without a live client; their errors propagate.
type (
	groupMembersFetcher func(ctx context.Context, createdSince string, pv servicenow.PaginationVars) ([]servicenow.GroupMember, string, error)
	userRolesFetcher    func(ctx context.Context, createdSince string, pv servicenow.PaginationVars) ([]servicenow.UserToRole, string, error)
	groupRolesFetcher   func(ctx context.Context, createdSince string, pv servicenow.PaginationVars) ([]servicenow.GroupToRole, string, error)
	// usersChangedFetcher polls sys_user by sys_updated_on (audit-INDEPENDENT):
	// it returns accounts created or modified at/after the cursor, backing the
	// account create/disable/modify phase.
	usersChangedFetcher func(ctx context.Context, changedSince string, pv servicenow.PaginationVars) ([]servicenow.User, string, error)
)

type serviceNowEventFeed struct {
	client *servicenow.Client

	// Grant-create phase fetchers (audit-INDEPENDENT real data source).
	fetchGroupMembers groupMembersFetcher
	fetchUserRoles    userRolesFetcher
	fetchGroupRoles   groupRolesFetcher
	fetchUsersChanged usersChangedFetcher

	// fetchJoinDeletes/fetchAudit back the two AUDIT-DEPENDENT phases. They
	// default to the client methods and are overridable in tests so a hard fetch
	// error can be injected to verify those phases are skipped (not fatal) while
	// the audit-independent grant-create phases keep producing events.
	fetchJoinDeletes auditDeleteFetcher
	fetchAudit       auditFetcher

	// fetchAuditDeletedTables backs the revoke-detection preflight: it reads the
	// glide.ui.audit_deleted_tables system property (the list of tables whose
	// deletes are captured to sys_audit_delete, the grant-REVOKE source).
	// Overridable in tests.
	fetchAuditDeletedTables func(ctx context.Context) ([]string, error)

	// preflightOnce guards the one-time advisory audit-config preflight warnings.
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

// revokeDetectionTables are the join (grant) tables whose hard deletes must be
// captured to sys_audit_delete for near-real-time grant-REVOKE events to fire.
// The signal is the glide.ui.audit_deleted_tables system property: a table's
// PRESENCE in that list means its deletes are reliably captured.
var revokeDetectionTables = []string{
	servicenow.TableUserGroupMember,
	servicenow.TableUserHasRole,
	servicenow.TableGroupHasRole,
}

// auditConfigPreflight runs, at most once per process, a single advisory check so
// silent non-detection of feed events is visible. It never fails the feed, and any
// failure of the check itself is swallowed with a debug log.
//
// The check is revoke detection (revokeDetectionPreflight). No account-side check
// is needed: account creates, disables, and other field changes are detected
// audit-independently by phaseUserChanges (sys_user by sys_updated_on), and
// account hard-deletes are logged to sys_audit regardless of any audit flag.
func (f *serviceNowEventFeed) auditConfigPreflight(ctx context.Context) {
	f.preflightOnce.Do(func() {
		f.revokeDetectionPreflight(ctx)
	})
}

// revokeDetectionPreflight is the feed's audit-config advisory check. It reads the
// glide.ui.audit_deleted_tables system property — the list of tables whose hard
// deletes are written to sys_audit_delete, the grant-REVOKE source. For each grant
// join table (sys_user_grmember / sys_user_has_role / sys_group_has_role): if it is
// NOT in the list, warn that near-real-time revoke detection is likely off for it
// (remediation: add the table to glide.ui.audit_deleted_tables; full-sync
// reconciliation still covers removals).
func (f *serviceNowEventFeed) revokeDetectionPreflight(ctx context.Context) {
	l := ctxzap.Extract(ctx)

	deletedTables, err := f.fetchAuditDeletedTables(ctx)
	if err != nil {
		l.Debug("baton-servicenow: event-feed revoke-detection preflight skipped (check failed)",
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

// feedPhase identifies which underlying source the cursor is currently draining.
// Phases run in a fixed order; the connector drains one phase fully (by offset)
// before advancing to the next, so the StreamToken cursor encodes (phase,
// offset).
type feedPhase int

const (
	phaseGroupMembers feedPhase = iota // sys_user_grmember -> group "member" grants
	phaseUserRoles                     // sys_user_has_role  -> role "member" grants
	phaseGroupRoles                    // sys_group_has_role -> role "member" grants
	phaseUserChanges                   // sys_user (by sys_updated_on) -> account create/disable/modify (RESOURCE_CHANGE)
	phaseJoinDeletes                   // sys_audit_delete   -> grant "revoke" events
	phaseAudit                         // sys_audit          -> account hard-delete (RESOURCE_CHANGE)
	phaseDone
)

// isAuditBackedPhase reports whether a phase reads from ServiceNow's audit
// tables (sys_audit_delete / sys_audit). These phases have a HARD dependency on
// instance audit configuration and access: a permission/ACL error on those
// tables must NOT kill the whole multi-source poll. The grant-CREATE phases
// (live join tables, no audit dependency) are NOT audit-backed — their errors
// propagate, because they are the real, always-available data source.
//
// (Note: "auditing disabled" returns EMPTY rows, not an error, and is already
// handled by the normal drained-phase path. This classification governs only
// HARD fetch errors — e.g. ACL/permission denials on the audit tables.)
func isAuditBackedPhase(p feedPhase) bool {
	return p == phaseJoinDeletes || p == phaseAudit
}

// feedCursor is the JSON-encoded StreamToken cursor for the audit feed.
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

// ListEvents returns the next page of events at or after earliestEvent. It walks
// the source tables phase by phase (grant-created phases first, then sys_audit
// for account/resource changes), using the StreamToken cursor to track the
// current phase and Table API offset, exactly mirroring Okta's cursor approach.
func (f *serviceNowEventFeed) ListEvents(
	ctx context.Context,
	earliestEvent *timestamppb.Timestamp,
	pToken *pagination.StreamToken,
) ([]*v2.Event, *pagination.StreamState, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	// Advisory, never-fatal: warn once per process if instance audit config means
	// the audit-backed phases (revokes / account changes) can't fire.
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
			// Phase isolation (we are MULTI-source, unlike okta's single-source
			// feed): a HARD error on an AUDIT-BACKED phase (locked-down or
			// inaccessible sys_audit / sys_audit_delete, ACL/permission denial)
			// must NOT kill the whole poll. Log a warning, advance the cursor PAST
			// this phase (reset offset), and continue the walk so the always-working
			// grant-create phases still return their events. The periodic full sync
			// remains the backstop for the skipped revoke/account-change source.
			//
			// Grant-create phases (live join tables, no audit dependency) are the
			// real data source: their errors still propagate.
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

		// Return whatever this phase yielded; HasMore stays true until phaseDone.
		if len(events) > 0 {
			state, encErr := streamState(cursor, cursor.Phase < phaseDone)
			if encErr != nil {
				return nil, nil, nil, encErr
			}
			return events, state, nil, nil
		}
		l.Debug("baton-servicenow: event-feed phase produced no events, advancing", zap.Int("phase", int(cursor.Phase-1)))
	}

	// All phases drained.
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

// fetchPhase fetches one page from the source backing the given phase and maps
// the rows to events.
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
		for i := range rows {
			if ev := joinDeleteRevokeEvent(ctx, &rows[i]); ev != nil {
				evs = append(evs, ev)
			}
		}
		return evs, next, nil

	case phaseAudit:
		rows, next, err := f.fetchAudit(ctx, servicenow.AuditedTables, since, pv)
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

// occurredAt parses a ServiceNow datetime literal into a proto timestamp,
// falling back to the current time if it cannot be parsed.
func occurredAt(snDatetime string) *timestamppb.Timestamp {
	if t, err := time.Parse(snDatetimeLayout, snDatetime); err == nil {
		return timestamppb.New(t.UTC())
	}
	return timestamppb.Now()
}

// groupMemberCreateGrantEvent maps a new sys_user_grmember row to a
// grant-created event: principal=user, entitlement=group "member".
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

// userRoleCreateGrantEvent maps a new sys_user_has_role row to a grant-created
// event: principal=user, entitlement=role "member".
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

// groupRoleCreateGrantEvent maps a new sys_group_has_role row to a grant-created
// event: principal=group, entitlement=role "member".
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

// joinDeleteTables are the membership/assignment join tables whose hard deletes
// the event feed turns into revoke events. Other deleted tables (e.g. sys_user)
// are ignored by the revoke phase (account deletes are reconciled by the sync /
// surface as a sys_user audit-delete handled elsewhere).
var joinDeleteTables = []string{
	servicenow.TableUserGroupMember,
	servicenow.TableUserHasRole,
	servicenow.TableGroupHasRole,
}

// deletedRowPayload is the parsed XML of a sys_audit_delete.payload column. The
// payload is the full serialization of the deleted row: each column is a child
// element of the (table-named) root whose text is the column's value, and a
// reference column's text is the referenced sys_id. We bind every field we may
// need across the join tables; only the relevant ones are set per table.
type deletedRowPayload struct {
	Group string `xml:"group"` // sys_user_grmember / sys_group_has_role
	User  string `xml:"user"`  // sys_user_grmember / sys_user_has_role
	Role  string `xml:"role"`  // sys_user_has_role / sys_group_has_role
}

// parseDeletedRowPayload parses a sys_audit_delete payload XML dump into its
// reference fields. The root element is the deleted row's table name, so we
// decode against the generic deletedRowPayload regardless of table.
func parseDeletedRowPayload(payload string) (*deletedRowPayload, error) {
	var p deletedRowPayload
	if err := xml.Unmarshal([]byte(payload), &p); err != nil {
		return nil, err
	}
	return &p, nil
}

// joinDeleteRevokeEvent maps a sys_audit_delete row for one of the join
// tables to a grant-revoke event, resolving (principal, entitlement) from the
// deleted row's full XML payload. Returns nil (and logs a warning) for rows it
// cannot map — a missing/unparseable payload, an unexpected table, or a payload
// missing the reference fields — so a single bad row never fails the feed.
func joinDeleteRevokeEvent(ctx context.Context, d *servicenow.AuditDeleteRecord) *v2.Event {
	l := ctxzap.Extract(ctx)

	if d.Payload == "" {
		l.Warn("baton-servicenow: skipping deleted grant with empty payload (delete-recovery may be disabled)",
			zap.String("table", d.Tablename),
			zap.String("documentkey", d.DocumentKey),
		)
		return nil
	}

	p, err := parseDeletedRowPayload(d.Payload)
	if err != nil {
		l.Warn("baton-servicenow: skipping deleted grant with unparseable payload",
			zap.String("table", d.Tablename),
			zap.String("documentkey", d.DocumentKey),
			zap.Error(err),
		)
		return nil
	}

	var principal *v2.Resource
	var entitlement *v2.Entitlement

	switch d.Tablename {
	case servicenow.TableUserGroupMember:
		// principal=user, entitlement=group "member".
		if p.User == "" || p.Group == "" {
			break
		}
		principal = minimalResource(resourceTypeUser, p.User)
		entitlement = ent.NewAssignmentEntitlement(minimalResource(resourceTypeGroup, p.Group), groupMembership)
	case servicenow.TableUserHasRole:
		// principal=user, entitlement=role "member".
		if p.User == "" || p.Role == "" {
			break
		}
		principal = minimalResource(resourceTypeUser, p.User)
		entitlement = ent.NewAssignmentEntitlement(minimalResource(resourceTypeRole, p.Role), roleMembership)
	case servicenow.TableGroupHasRole:
		// principal=group, entitlement=role "member".
		if p.Group == "" || p.Role == "" {
			break
		}
		principal = minimalResource(resourceTypeGroup, p.Group)
		entitlement = ent.NewAssignmentEntitlement(minimalResource(resourceTypeRole, p.Role), roleMembership)
	default:
		// Not a join table we map; ignore quietly.
		return nil
	}

	if principal == nil || entitlement == nil {
		l.Warn("baton-servicenow: skipping deleted grant with incomplete payload references",
			zap.String("table", d.Tablename),
			zap.String("documentkey", d.DocumentKey),
		)
		return nil
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
	}
}

// auditChangeEvent maps a sys_audit row to a resource-change event when the
// changed record maps cleanly to a connector resource. In practice this is the
// account hard-DELETE path: a deleted sys_user writes a sys_audit "DELETED" row
// (documentkey == user sys_id) regardless of the table's audit flag, which emits
// a ResourceChangeEvent so ConductorOne re-syncs (and drops) that account.
// Account creates and field changes — including active=false disables — are
// covered audit-independently by phaseUserChanges (sys_user by sys_updated_on);
// if field-change auditing happens to be enabled, those rows would also appear
// here, producing a duplicate but idempotent ResourceChangeEvent.
//
// Rows on the join tables are skipped here (returns nil): their sys_audit
// documentkey is only the join-row sys_id. Grant CREATIONS are surfaced by the
// dedicated membership-table phases above, and grant DELETES are surfaced as
// CreateRevokeEvents by phaseJoinDeletes (which reads sys_audit_delete.payload —
// the full XML of the deleted row — to resolve the (principal, entitlement)
// pair, something the sys_audit row alone cannot do).
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

// userChangeEvent maps a sys_user row found by polling sys_user by
// sys_updated_on to a RESOURCE_CHANGE event so ConductorOne re-syncs that
// account. This is the audit-INDEPENDENT source for account CREATES and field
// changes — notably active=false DISABLES: ServiceNow writes no sys_audit row
// for inserts (glide.sys.audit_inserts is false) and writes field-change rows
// only when table auditing is enabled, so without this phase neither new nor
// disabled accounts would surface in near-real-time. Account hard-DELETES are
// covered by phaseAudit instead (the sys_audit "DELETED" row, written regardless
// of the audit flag); a deleted account no longer exists in sys_user, so it
// cannot appear here.
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

// minimalResource builds a resource carrying only its id/type, sufficient for
// event payloads (ConductorOne resolves full attributes from its synced graph).
func minimalResource(rt *v2.ResourceType, id string) *v2.Resource {
	r, err := rs.NewResource(id, rt, id)
	if err != nil {
		// NewResource only errors on option application; with no options it
		// cannot fail, but stay defensive and fall back to a bare resource.
		return &v2.Resource{Id: &v2.ResourceId{ResourceType: rt.Id, Resource: id}}
	}
	return r
}
