package connector

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-servicenow/pkg/servicenow"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// ctxWithObservedLogs returns a context carrying an observed zap logger and the
// observed-logs sink, so tests can assert on what the feed logged.
func ctxWithObservedLogs(t *testing.T) (context.Context, *observer.ObservedLogs) {
	t.Helper()
	core, logs := observer.New(zapcore.DebugLevel)
	ctx := ctxzap.ToContext(t.Context(), zap.New(core))
	return ctx, logs
}

// drainFeed walks ListEvents to completion (HasMore=false), returning every
// event collected across the paged calls. It guards against an infinite loop.
func drainFeed(t *testing.T, ctx context.Context, f *serviceNowEventFeed) []*v2.Event {
	t.Helper()
	var all []*v2.Event
	tok := &pagination.StreamToken{Size: 100}
	for i := 0; i < 100; i++ {
		evs, state, _, err := f.ListEvents(ctx, nil, tok)
		if err != nil {
			t.Fatalf("ListEvents returned error: %v", err)
		}
		all = append(all, evs...)
		if state == nil || !state.HasMore {
			return all
		}
		tok = &pagination.StreamToken{Size: 100, Cursor: state.Cursor}
	}
	t.Fatal("drainFeed did not terminate")
	return nil
}

// emptyFetchersFeed builds a feed whose every phase returns no rows and no
// error, so tests can override only the phases they exercise.
func emptyFetchersFeed() *serviceNowEventFeed {
	return &serviceNowEventFeed{
		fetchGroupMembers: func(context.Context, string, servicenow.PaginationVars) ([]servicenow.GroupMember, string, error) {
			return nil, "", nil
		},
		fetchUserRoles: func(context.Context, string, servicenow.PaginationVars) ([]servicenow.UserToRole, string, error) {
			return nil, "", nil
		},
		fetchGroupRoles: func(context.Context, string, servicenow.PaginationVars) ([]servicenow.GroupToRole, string, error) {
			return nil, "", nil
		},
		fetchUsersChanged: func(context.Context, string, servicenow.PaginationVars) ([]servicenow.User, string, error) {
			return nil, "", nil
		},
		fetchJoinDeletes: func(context.Context, []string, string, servicenow.PaginationVars) ([]servicenow.AuditDeleteRecord, string, error) {
			return nil, "", nil
		},
		fetchAudit: func(context.Context, []string, string, servicenow.PaginationVars) ([]servicenow.AuditRecord, string, error) {
			return nil, "", nil
		},
		fetchAuditDeletedTables: func(context.Context) ([]string, error) {
			// Default: all three grant join tables captured to sys_audit_delete (no
			// revoke warning) unless a test overrides it.
			return []string{
				servicenow.TableUserGroupMember,
				servicenow.TableUserHasRole,
				servicenow.TableGroupHasRole,
			}, nil
		},
	}
}

func TestGroupMemberCreateGrantEvent(t *testing.T) {
	m := &servicenow.GroupMember{
		BaseResource: servicenow.BaseResource{Id: "join1"},
		User:         "user1",
		Group:        "group1",
		SysCreatedOn: "2026-06-18 10:00:00",
	}
	ev := groupMemberCreateGrantEvent(m)

	if ev.GetId() != "grmember:join1" {
		t.Fatalf("unexpected event id: %s", ev.GetId())
	}
	cg := ev.GetCreateGrantEvent()
	if cg == nil {
		t.Fatal("expected CreateGrantEvent")
	}
	if got := cg.GetPrincipal().GetId(); got.GetResourceType() != resourceTypeUser.Id || got.GetResource() != "user1" {
		t.Fatalf("unexpected principal: %v", got)
	}
	entRes := cg.GetEntitlement().GetResource().GetId()
	if entRes.GetResourceType() != resourceTypeGroup.Id || entRes.GetResource() != "group1" {
		t.Fatalf("unexpected entitlement resource: %v", entRes)
	}
	if cg.GetEntitlement().GetSlug() != groupMembership {
		t.Fatalf("unexpected entitlement slug: %s", cg.GetEntitlement().GetSlug())
	}
	if ev.GetOccurredAt().AsTime().Format(snDatetimeLayout) != "2026-06-18 10:00:00" {
		t.Fatalf("unexpected occurred_at: %s", ev.GetOccurredAt().AsTime())
	}
}

func TestUserRoleCreateGrantEvent(t *testing.T) {
	r := &servicenow.UserToRole{
		BaseResource: servicenow.BaseResource{Id: "ur1"},
		User:         "user2",
		Role:         "role2",
		SysCreatedOn: "2026-06-18 11:30:00",
	}
	ev := userRoleCreateGrantEvent(r)
	cg := ev.GetCreateGrantEvent()
	if cg == nil {
		t.Fatal("expected CreateGrantEvent")
	}
	if cg.GetPrincipal().GetId().GetResource() != "user2" {
		t.Fatalf("unexpected principal: %v", cg.GetPrincipal().GetId())
	}
	er := cg.GetEntitlement().GetResource().GetId()
	if er.GetResourceType() != resourceTypeRole.Id || er.GetResource() != "role2" {
		t.Fatalf("unexpected entitlement resource: %v", er)
	}
	if cg.GetEntitlement().GetSlug() != roleMembership {
		t.Fatalf("unexpected slug: %s", cg.GetEntitlement().GetSlug())
	}
}

func TestGroupRoleCreateGrantEvent(t *testing.T) {
	r := &servicenow.GroupToRole{
		BaseResource: servicenow.BaseResource{Id: "gr1"},
		Group:        "group3",
		Role:         "role3",
		SysCreatedOn: "2026-06-18 12:00:00",
	}
	ev := groupRoleCreateGrantEvent(r)
	cg := ev.GetCreateGrantEvent()
	if cg == nil {
		t.Fatal("expected CreateGrantEvent")
	}
	if p := cg.GetPrincipal().GetId(); p.GetResourceType() != resourceTypeGroup.Id || p.GetResource() != "group3" {
		t.Fatalf("unexpected principal: %v", p)
	}
	if er := cg.GetEntitlement().GetResource().GetId(); er.GetResource() != "role3" {
		t.Fatalf("unexpected entitlement resource: %v", er)
	}
}

func TestAuditChangeEvent_UserChange(t *testing.T) {
	a := &servicenow.AuditRecord{
		SysID:        "aud1",
		Tablename:    servicenow.TableUser,
		DocumentKey:  "user5",
		Fieldname:    "active",
		OldValue:     "true",
		NewValue:     "false",
		SysCreatedOn: "2026-06-18 14:00:00",
	}
	ev := auditChangeEvent(a)
	if ev == nil {
		t.Fatal("expected an event for a sys_user change")
	}
	if ev.GetId() != "audit:aud1" {
		t.Fatalf("unexpected id: %s", ev.GetId())
	}
	rc := ev.GetResourceChangeEvent()
	if rc == nil {
		t.Fatal("expected ResourceChangeEvent")
	}
	if rc.GetResourceId().GetResourceType() != resourceTypeUser.Id || rc.GetResourceId().GetResource() != "user5" {
		t.Fatalf("unexpected resource id: %v", rc.GetResourceId())
	}
}

func TestAuditChangeEvent_NonUserSkipped(t *testing.T) {
	// A delete on a join table is not resolvable to a grant; skip it here.
	a := &servicenow.AuditRecord{
		SysID:        "aud2",
		Tablename:    servicenow.TableUserGroupMember,
		DocumentKey:  "joinrow",
		Fieldname:    "DELETED",
		SysCreatedOn: "2026-06-18 15:00:00",
	}
	if ev := auditChangeEvent(a); ev != nil {
		t.Fatalf("expected nil event for join-table audit row, got %v", ev)
	}
}

// Real sys_audit_delete.payload XML samples verified on dev289997. Each
// reference field is an element whose TEXT is the referenced sys_id (with a
// display_value attribute we ignore).
const (
	grmemberDeletePayload = `<sys_user_grmember><group display_value="G">5338c33d83290f5442cb56d6feaad3d9</group>` +
		`<user display_value="U">0738c33d83290f5442cb56d6feaad3d1</user></sys_user_grmember>`

	userHasRoleDeletePayload = `<sys_user_has_role><role display_value="admin">role0001role0001role0001role00010</role>` +
		`<user display_value="U">0738c33d83290f5442cb56d6feaad3d1</user></sys_user_has_role>`

	groupHasRoleDeletePayload = `<sys_group_has_role><group display_value="G">5338c33d83290f5442cb56d6feaad3d9</group>` +
		`<role display_value="admin">role0001role0001role0001role00010</role></sys_group_has_role>`
)

func TestJoinDeleteRevokeEvent_GroupMember(t *testing.T) {
	d := &servicenow.AuditDeleteRecord{
		Tablename:    servicenow.TableUserGroupMember,
		DocumentKey:  "join1",
		SysCreatedOn: "2026-06-18 16:00:00",
		Payload:      grmemberDeletePayload,
	}
	ev := joinDeleteRevokeEvent(context.Background(), d)
	if ev == nil {
		t.Fatal("expected a revoke event")
	}
	if ev.GetId() != "revoke:join1" {
		t.Fatalf("unexpected id: %s", ev.GetId())
	}
	rv := ev.GetCreateRevokeEvent()
	if rv == nil {
		t.Fatal("expected CreateRevokeEvent")
	}
	if p := rv.GetPrincipal().GetId(); p.GetResourceType() != resourceTypeUser.Id || p.GetResource() != "0738c33d83290f5442cb56d6feaad3d1" {
		t.Fatalf("unexpected principal: %v", p)
	}
	er := rv.GetEntitlement().GetResource().GetId()
	if er.GetResourceType() != resourceTypeGroup.Id || er.GetResource() != "5338c33d83290f5442cb56d6feaad3d9" {
		t.Fatalf("unexpected entitlement resource: %v", er)
	}
	if rv.GetEntitlement().GetSlug() != groupMembership {
		t.Fatalf("unexpected slug: %s", rv.GetEntitlement().GetSlug())
	}
	if ev.GetOccurredAt().AsTime().Format(snDatetimeLayout) != "2026-06-18 16:00:00" {
		t.Fatalf("unexpected occurred_at: %s", ev.GetOccurredAt().AsTime())
	}
}

func TestJoinDeleteRevokeEvent_UserHasRole(t *testing.T) {
	d := &servicenow.AuditDeleteRecord{
		Tablename:   servicenow.TableUserHasRole,
		DocumentKey: "join2",
		Payload:     userHasRoleDeletePayload,
	}
	ev := joinDeleteRevokeEvent(context.Background(), d)
	if ev == nil {
		t.Fatal("expected a revoke event")
	}
	rv := ev.GetCreateRevokeEvent()
	if p := rv.GetPrincipal().GetId(); p.GetResourceType() != resourceTypeUser.Id || p.GetResource() != "0738c33d83290f5442cb56d6feaad3d1" {
		t.Fatalf("unexpected principal: %v", p)
	}
	er := rv.GetEntitlement().GetResource().GetId()
	if er.GetResourceType() != resourceTypeRole.Id || er.GetResource() != "role0001role0001role0001role00010" {
		t.Fatalf("unexpected entitlement resource: %v", er)
	}
	if rv.GetEntitlement().GetSlug() != roleMembership {
		t.Fatalf("unexpected slug: %s", rv.GetEntitlement().GetSlug())
	}
}

func TestJoinDeleteRevokeEvent_GroupHasRole(t *testing.T) {
	d := &servicenow.AuditDeleteRecord{
		Tablename:   servicenow.TableGroupHasRole,
		DocumentKey: "join3",
		Payload:     groupHasRoleDeletePayload,
	}
	ev := joinDeleteRevokeEvent(context.Background(), d)
	if ev == nil {
		t.Fatal("expected a revoke event")
	}
	rv := ev.GetCreateRevokeEvent()
	// Group is the principal for sys_group_has_role.
	if p := rv.GetPrincipal().GetId(); p.GetResourceType() != resourceTypeGroup.Id || p.GetResource() != "5338c33d83290f5442cb56d6feaad3d9" {
		t.Fatalf("unexpected principal: %v", p)
	}
	er := rv.GetEntitlement().GetResource().GetId()
	if er.GetResourceType() != resourceTypeRole.Id || er.GetResource() != "role0001role0001role0001role00010" {
		t.Fatalf("unexpected entitlement resource: %v", er)
	}
	if rv.GetEntitlement().GetSlug() != roleMembership {
		t.Fatalf("unexpected slug: %s", rv.GetEntitlement().GetSlug())
	}
}

func TestJoinDeleteRevokeEvent_EmptyPayloadSkipped(t *testing.T) {
	d := &servicenow.AuditDeleteRecord{
		Tablename:   servicenow.TableUserGroupMember,
		DocumentKey: "join5",
		Payload:     "",
	}
	if ev := joinDeleteRevokeEvent(context.Background(), d); ev != nil {
		t.Fatalf("expected nil for empty payload, got %v", ev)
	}
}

func TestJoinDeleteRevokeEvent_UnparseablePayloadSkipped(t *testing.T) {
	d := &servicenow.AuditDeleteRecord{
		Tablename:   servicenow.TableUserGroupMember,
		DocumentKey: "join6",
		Payload:     "<not-valid-xml<<<",
	}
	if ev := joinDeleteRevokeEvent(context.Background(), d); ev != nil {
		t.Fatalf("expected nil for unparseable payload, got %v", ev)
	}
}

func TestJoinDeleteRevokeEvent_IncompletePayloadSkipped(t *testing.T) {
	// Payload parses but is missing a reference field.
	d := &servicenow.AuditDeleteRecord{
		Tablename:   servicenow.TableUserGroupMember,
		DocumentKey: "join7",
		Payload:     `<sys_user_grmember><user>u1</user></sys_user_grmember>`,
	}
	if ev := joinDeleteRevokeEvent(context.Background(), d); ev != nil {
		t.Fatalf("expected nil for incomplete payload, got %v", ev)
	}
}

func TestJoinDeleteRevokeEvent_UnknownTableSkipped(t *testing.T) {
	d := &servicenow.AuditDeleteRecord{
		Tablename:   servicenow.TableUser,
		DocumentKey: "u1",
		Payload:     `<sys_user><user_name>jdoe</user_name></sys_user>`,
	}
	if ev := joinDeleteRevokeEvent(context.Background(), d); ev != nil {
		t.Fatalf("expected nil for non-join table, got %v", ev)
	}
}

func TestOccurredAt_FallbackOnUnparseable(t *testing.T) {
	ts := occurredAt("not-a-date")
	if ts == nil || ts.AsTime().IsZero() {
		t.Fatal("expected a non-zero fallback timestamp")
	}
}

func TestFeedCursor_RoundTrip(t *testing.T) {
	c := feedCursor{Phase: phaseUserRoles, Offset: 200}
	enc, err := c.encode()
	if err != nil {
		t.Fatal(err)
	}
	got, err := parseFeedCursor(enc)
	if err != nil {
		t.Fatal(err)
	}
	if got != c {
		t.Fatalf("round trip mismatch: %+v != %+v", got, c)
	}

	// Empty cursor starts at the first phase, offset 0.
	zero, err := parseFeedCursor("")
	if err != nil {
		t.Fatal(err)
	}
	if zero.Phase != phaseGroupMembers || zero.Offset != 0 {
		t.Fatalf("unexpected zero cursor: %+v", zero)
	}
}

func TestUserChangeEvent(t *testing.T) {
	u := &servicenow.User{
		BaseResource: servicenow.BaseResource{Id: "user42"},
		SysUpdatedOn: "2026-06-19 07:14:22",
	}
	ev := userChangeEvent(u)

	if ev.GetId() != "userchange:user42" {
		t.Fatalf("unexpected event id: %s", ev.GetId())
	}
	rc := ev.GetResourceChangeEvent()
	if rc == nil {
		t.Fatal("expected ResourceChangeEvent")
	}
	if got := rc.GetResourceId(); got.GetResourceType() != resourceTypeUser.Id || got.GetResource() != "user42" {
		t.Fatalf("unexpected resource id: %v", got)
	}
	if ev.GetOccurredAt().AsTime().Format(snDatetimeLayout) != "2026-06-19 07:14:22" {
		t.Fatalf("unexpected occurred_at: %s", ev.GetOccurredAt().AsTime())
	}
}

// TestListEvents_UserChangePhase verifies the audit-INDEPENDENT account-change
// phase: polling sys_user by sys_updated_on surfaces both a newly created account
// and a disabled (active=false) account as RESOURCE_CHANGE events, with no
// sys_audit dependency.
func TestListEvents_UserChangePhase(t *testing.T) {
	ctx, _ := ctxWithObservedLogs(t)

	f := emptyFetchersFeed()
	f.fetchUsersChanged = func(context.Context, string, servicenow.PaginationVars) ([]servicenow.User, string, error) {
		return []servicenow.User{
			{BaseResource: servicenow.BaseResource{Id: "newuser1"}, Active: "true", SysUpdatedOn: "2026-06-19 07:14:22"},   // created
			{BaseResource: servicenow.BaseResource{Id: "disabled1"}, Active: "false", SysUpdatedOn: "2026-06-19 07:30:37"}, // disabled
		}, "", nil
	}

	events := drainFeed(t, ctx, f)

	if len(events) != 2 {
		t.Fatalf("expected 2 resource-change events, got %d", len(events))
	}
	ids := map[string]bool{}
	for _, ev := range events {
		rc := ev.GetResourceChangeEvent()
		if rc == nil {
			t.Fatalf("expected ResourceChangeEvent, got %+v", ev)
		}
		if rc.GetResourceId().GetResourceType() != resourceTypeUser.Id {
			t.Fatalf("unexpected resource type: %v", rc.GetResourceId())
		}
		ids[rc.GetResourceId().GetResource()] = true
	}
	if !ids["newuser1"] || !ids["disabled1"] {
		t.Fatalf("expected both created and disabled accounts, got %v", ids)
	}
}

// --- Change A: phase isolation (resilience) ---

// TestListEvents_AuditPhaseErrorIsolated verifies that a HARD error from BOTH
// audit-backed phases (sys_audit_delete revokes and sys_audit account changes)
// is logged-and-skipped rather than fatal: the grant-create phases still return
// their events, the walk completes (cursor reaches phaseDone), and no grant-
// create events are lost.
func TestListEvents_AuditPhaseErrorIsolated(t *testing.T) {
	ctx, logs := ctxWithObservedLogs(t)

	f := emptyFetchersFeed()
	// One grant-create event from the very first phase.
	f.fetchGroupMembers = func(context.Context, string, servicenow.PaginationVars) ([]servicenow.GroupMember, string, error) {
		return []servicenow.GroupMember{
			{BaseResource: servicenow.BaseResource{Id: "join1"}, User: "u1", Group: "g1", SysCreatedOn: "2026-06-18 10:00:00"},
		}, "", nil
	}
	// Both audit-backed phases hard-error (e.g. ACL/permission denial).
	auditErr := errors.New("403 ACL Exception: read on sys_audit_delete denied")
	f.fetchJoinDeletes = func(context.Context, []string, string, servicenow.PaginationVars) ([]servicenow.AuditDeleteRecord, string, error) {
		return nil, "", auditErr
	}
	f.fetchAudit = func(context.Context, []string, string, servicenow.PaginationVars) ([]servicenow.AuditRecord, string, error) {
		return nil, "", auditErr
	}

	events := drainFeed(t, ctx, f)

	// The grant-create event survives the audit-phase failures.
	if len(events) != 1 {
		t.Fatalf("expected exactly 1 grant-create event, got %d", len(events))
	}
	if events[0].GetId() != "grmember:join1" || events[0].GetCreateGrantEvent() == nil {
		t.Fatalf("unexpected surviving event: %+v", events[0])
	}

	// Each skipped audit-backed phase logged a warning (2 phases -> 2 warns).
	warns := logs.FilterLevelExact(zapcore.WarnLevel).
		FilterMessageSnippet("skipping audit-backed event-feed phase").All()
	if len(warns) != 2 {
		t.Fatalf("expected 2 skipped-phase warnings, got %d: %+v", len(warns), warns)
	}
}

// TestListEvents_GrantCreateErrorPropagates verifies that an error on a
// grant-CREATE phase (the real, audit-independent data source) is NOT swallowed:
// it propagates out of ListEvents.
func TestListEvents_GrantCreateErrorPropagates(t *testing.T) {
	ctx, _ := ctxWithObservedLogs(t)

	f := emptyFetchersFeed()
	boom := errors.New("network is down")
	f.fetchUserRoles = func(context.Context, string, servicenow.PaginationVars) ([]servicenow.UserToRole, string, error) {
		return nil, "", boom
	}

	tok := &pagination.StreamToken{Size: 100}
	for i := 0; i < 100; i++ {
		_, state, _, err := f.ListEvents(ctx, nil, tok)
		if err != nil {
			if !strings.Contains(err.Error(), "network is down") {
				t.Fatalf("unexpected error: %v", err)
			}
			return // propagated, as required
		}
		if state == nil || !state.HasMore {
			t.Fatal("expected grant-create phase error to propagate, but the walk completed cleanly")
		}
		tok = &pagination.StreamToken{Size: 100, Cursor: state.Cursor}
	}
	t.Fatal("did not terminate")
}

// TestListEvents_SkippedPhaseCursorAdvances verifies the cursor advances exactly
// one phase past a skipped audit phase (no stall, no re-loop, no dropped later
// phase): with phaseJoinDeletes failing, the subsequent phaseAudit still runs
// and its event is returned.
func TestListEvents_SkippedPhaseCursorAdvances(t *testing.T) {
	ctx, _ := ctxWithObservedLogs(t)

	f := emptyFetchersFeed()
	// Revoke phase errors; audit phase (the NEXT phase) succeeds with one event.
	f.fetchJoinDeletes = func(context.Context, []string, string, servicenow.PaginationVars) ([]servicenow.AuditDeleteRecord, string, error) {
		return nil, "", errors.New("sys_audit_delete locked down")
	}
	f.fetchAudit = func(context.Context, []string, string, servicenow.PaginationVars) ([]servicenow.AuditRecord, string, error) {
		return []servicenow.AuditRecord{
			{SysID: "a1", Tablename: servicenow.TableUser, DocumentKey: "user9", Fieldname: "DELETED", SysCreatedOn: "2026-06-18 11:00:00"},
		}, "", nil
	}

	events := drainFeed(t, ctx, f)
	if len(events) != 1 || events[0].GetResourceChangeEvent() == nil {
		t.Fatalf("expected the post-skip phaseAudit event to survive; got %d events: %+v", len(events), events)
	}
	if got := events[0].GetResourceChangeEvent().GetResourceId().GetResource(); got != "user9" {
		t.Fatalf("unexpected resource-change id: %s", got)
	}
}

// --- Change B: audit-config preflight warnings ---

// --- Check 1: revoke detection (glide.ui.audit_deleted_tables) ---

// joinTablesPresent is the real dev289997 property value (the three grant join
// tables ARE present, so no revoke warning should fire).
func joinTablesPresent() []string {
	return []string{
		servicenow.TableUserGroupMember,
		servicenow.TableUserHasRole,
		servicenow.TableGroupHasRole,
		// Other tables ServiceNow lists.
		"sys_user",
		"sys_user_group",
		"sys_user_role",
	}
}

func TestRevokeDetectionPreflight_JoinTablesPresent_NoWarn(t *testing.T) {
	ctx, logs := ctxWithObservedLogs(t)

	f := emptyFetchersFeed()
	f.fetchAuditDeletedTables = func(context.Context) ([]string, error) {
		return joinTablesPresent(), nil
	}

	f.revokeDetectionPreflight(ctx)

	if n := logs.FilterLevelExact(zapcore.WarnLevel).Len(); n != 0 {
		t.Fatalf("expected no revoke warning when all join tables are captured, got %d", n)
	}
}

func TestRevokeDetectionPreflight_JoinTableMissing_Warns(t *testing.T) {
	ctx, logs := ctxWithObservedLogs(t)

	f := emptyFetchersFeed()
	f.fetchAuditDeletedTables = func(context.Context) ([]string, error) {
		// sys_group_has_role missing from the captured list.
		return []string{
			servicenow.TableUserGroupMember,
			servicenow.TableUserHasRole,
		}, nil
	}

	f.revokeDetectionPreflight(ctx)

	warns := logs.FilterLevelExact(zapcore.WarnLevel).
		FilterMessageSnippet("delete-capture is NOT enabled").All()
	if len(warns) != 1 {
		t.Fatalf("expected 1 revoke warning for the missing join table, got %d", len(warns))
	}
	found := false
	for _, fld := range warns[0].Context {
		if fld.Key == "uncaptured_tables" {
			found = true
			// zap.Strings renders as a zapcore.ArrayMarshaler; check via its
			// encoded form rather than a concrete []string cast.
			if got := fmt.Sprint(fld.Interface); !strings.Contains(got, servicenow.TableGroupHasRole) {
				t.Fatalf("expected uncaptured_tables to name %s, got %q", servicenow.TableGroupHasRole, got)
			}
			if strings.Contains(fmt.Sprint(fld.Interface), servicenow.TableUserGroupMember) {
				t.Fatalf("captured table %s should not be in uncaptured_tables", servicenow.TableUserGroupMember)
			}
		}
	}
	if !found {
		t.Fatalf("warning did not name the uncaptured table(s): %+v", warns[0].Context)
	}
}

func TestRevokeDetectionPreflight_CheckFailureSwallowed(t *testing.T) {
	ctx, logs := ctxWithObservedLogs(t)

	f := emptyFetchersFeed()
	f.fetchAuditDeletedTables = func(context.Context) ([]string, error) {
		return nil, errors.New("sys_properties unreadable")
	}

	f.revokeDetectionPreflight(ctx)

	if n := logs.FilterLevelExact(zapcore.WarnLevel).Len(); n != 0 {
		t.Fatalf("a failed revoke-detection check must not warn, got %d warnings", n)
	}
}

func TestAuditConfigPreflight_RunsOnce(t *testing.T) {
	ctx, logs := ctxWithObservedLogs(t)

	calls := 0
	f := emptyFetchersFeed()
	f.fetchAuditDeletedTables = func(context.Context) ([]string, error) {
		calls++
		// Omit a grant join table so the single advisory check warns once.
		return []string{servicenow.TableUserHasRole, servicenow.TableGroupHasRole}, nil
	}

	f.auditConfigPreflight(ctx)
	f.auditConfigPreflight(ctx)
	f.auditConfigPreflight(ctx)

	if calls != 1 {
		t.Fatalf("expected preflight to run exactly once, ran %d times", calls)
	}
	if n := logs.FilterLevelExact(zapcore.WarnLevel).Len(); n != 1 {
		t.Fatalf("expected exactly 1 warning across repeated calls, got %d", n)
	}
}

func TestEventFeedMetadata(t *testing.T) {
	f := &serviceNowEventFeed{}
	md := f.EventFeedMetadata(t.Context())
	if md.GetId() != servicenowEventFeedID {
		t.Fatalf("unexpected feed id: %s", md.GetId())
	}
	want := map[v2.EventType]bool{
		v2.EventType_EVENT_TYPE_CREATE_GRANT:    false,
		v2.EventType_EVENT_TYPE_CREATE_REVOKE:   false,
		v2.EventType_EVENT_TYPE_RESOURCE_CHANGE: false,
	}
	for _, et := range md.GetSupportedEventTypes() {
		if _, ok := want[et]; ok {
			want[et] = true
		}
	}
	for et, seen := range want {
		if !seen {
			t.Fatalf("missing supported event type: %v", et)
		}
	}
}
