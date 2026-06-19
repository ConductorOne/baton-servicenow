package incremental

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/conductorone/baton-servicenow/pkg/servicenow"
)

// fakeDeleter returns a fixed set of audit-delete rows (or an error) for tests.
type fakeDeleter struct {
	records []servicenow.AuditDeleteRecord
	err     error
	since   string // captures the watermark Reconcile passed in
	calls   int
}

func (f *fakeDeleter) GetAllDeletedSince(_ context.Context, _ []string, createdSince string) ([]servicenow.AuditDeleteRecord, error) {
	f.calls++
	f.since = createdSince
	if f.err != nil {
		return nil, f.err
	}
	return f.records, nil
}

func del(table, key, createdOn string) servicenow.AuditDeleteRecord {
	return servicenow.AuditDeleteRecord{Tablename: table, DocumentKey: key, SysCreatedOn: createdOn}
}

func user(id, updated string) servicenow.User {
	u := servicenow.User{SysUpdatedOn: updated}
	u.Id = id
	return u
}

func TestDisabledIsNoOp(t *testing.T) {
	s, err := Load(t.TempDir(), "dev1", false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if s.Enabled() {
		t.Fatal("expected disabled")
	}
	if got := s.Watermark(StreamUsers); got != "" {
		t.Fatalf("disabled watermark should be empty, got %q", got)
	}
	in := []servicenow.User{user("a", "2026-01-01 00:00:00")}
	// MergeUsers must return the input unchanged and store nothing.
	out, err := s.MergeUsers(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 || out[0].Id != "a" {
		t.Fatalf("disabled merge should echo input, got %+v", out)
	}
	if err := s.Save(); err != nil {
		t.Fatalf("disabled Save should be no-op, got %v", err)
	}
}

func TestFirstSyncThenIncrementalMergeAndWatermark(t *testing.T) {
	dir := t.TempDir()

	// First sync: no state file -> empty snapshot, watermark empty (full pull).
	s, err := Load(dir, "dev1", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !s.Enabled() {
		t.Fatal("expected enabled")
	}
	if got := s.Watermark(StreamUsers); got != "" {
		t.Fatalf("first-sync watermark should be empty, got %q", got)
	}

	full := []servicenow.User{
		user("a", "2026-01-01 10:00:00"),
		user("b", "2026-01-02 11:00:00"),
	}
	merged, err := s.MergeUsers(full)
	if err != nil {
		t.Fatal(err)
	}
	if len(merged) != 2 {
		t.Fatalf("expected 2 merged users, got %d", len(merged))
	}

	// Second run: reload, watermark should be the max sys_updated_on seen.
	// Merge persists inline, so no explicit Save is required.
	s2, err := Load(dir, "dev1", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := s2.Watermark(StreamUsers); got != "2026-01-02 11:00:00" {
		t.Fatalf("watermark not advanced to max, got %q", got)
	}

	// Incremental delta: user "b" changed, plus a new user "c".
	delta := []servicenow.User{
		user("b", "2026-01-05 09:00:00"),
		user("c", "2026-01-06 08:00:00"),
	}
	merged2, err := s2.MergeUsers(delta)
	if err != nil {
		t.Fatal(err)
	}
	// Union must include the unchanged "a" carried forward from the snapshot.
	ids := map[string]bool{}
	for _, u := range merged2 {
		ids[u.Id] = true
	}
	if !ids["a"] || !ids["b"] || !ids["c"] || len(merged2) != 3 {
		t.Fatalf("expected union {a,b,c}, got %v (n=%d)", ids, len(merged2))
	}

	s3, err := Load(dir, "dev1", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := s3.Watermark(StreamUsers); got != "2026-01-06 08:00:00" {
		t.Fatalf("watermark not advanced after delta, got %q", got)
	}
}

func TestFailedRunDoesNotPersist(t *testing.T) {
	dir := t.TempDir()
	s, err := Load(dir, "dev1", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	// MarkFailed before the merge: the merge's inline persist must be a no-op.
	s.MarkFailed()
	if _, err := s.MergeUsers([]servicenow.User{user("a", "2026-01-01 10:00:00")}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "baton-servicenow-incremental-dev1.json")
	s2, _ := Load(dir, "dev1", true, nil)
	if s2.Watermark(StreamUsers) != "" {
		t.Fatalf("failed run must not advance watermark; file=%s", path)
	}
}

func TestCorruptStateForcesFullPull(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "baton-servicenow-incremental-dev1.json")
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatal(err)
	}
	s, err := Load(dir, "dev1", true, nil)
	if err != nil {
		t.Fatalf("corrupt cache must not error, got %v", err)
	}
	if s.Watermark(StreamUsers) != "" {
		t.Fatal("corrupt cache should yield empty watermark (full pull)")
	}
}

func TestGroupMemberMergePerGroup(t *testing.T) {
	dir := t.TempDir()
	s, _ := Load(dir, "dev1", true, nil)

	gm := func(id, updated string) servicenow.GroupMember {
		m := servicenow.GroupMember{User: "u-" + id, SysUpdatedOn: updated}
		m.Id = id
		return m
	}
	if _, err := s.MergeGroupMembers("g1", []servicenow.GroupMember{gm("m1", "2026-01-01 00:00:00")}); err != nil {
		t.Fatal(err)
	}
	out, err := s.MergeGroupMembers("g1", []servicenow.GroupMember{gm("m2", "2026-01-02 00:00:00")})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 2 {
		t.Fatalf("expected 2 members for g1, got %d", len(out))
	}
	// A different group is isolated.
	out2, err := s.MergeGroupMembers("g2", []servicenow.GroupMember{gm("m3", "2026-01-03 00:00:00")})
	if err != nil {
		t.Fatal(err)
	}
	if len(out2) != 1 {
		t.Fatalf("expected 1 member for g2, got %d", len(out2))
	}
}

// seedSnapshot builds a snapshot with one of each resource and one join row in
// each nested map, for prune tests.
func seedSnapshot() *Snapshot {
	snap := newSnapshot("dev1")
	snap.Users["u1"] = user("u1", "2026-01-01 00:00:00")
	snap.Groups["g1"] = servicenow.Group{BaseResource: servicenow.BaseResource{Id: "g1"}}
	snap.Roles["r1"] = servicenow.Role{BaseResource: servicenow.BaseResource{Id: "r1"}}
	snap.GroupMembers["g1"] = map[string]servicenow.GroupMember{
		"gm1": {BaseResource: servicenow.BaseResource{Id: "gm1"}, User: "u1", Group: "g1"},
		"gm2": {BaseResource: servicenow.BaseResource{Id: "gm2"}, User: "u2", Group: "g1"},
	}
	snap.UserRoles["r1"] = map[string]servicenow.UserToRole{
		"ur1": {BaseResource: servicenow.BaseResource{Id: "ur1"}, User: "u1", Role: "r1"},
	}
	snap.GroupRoles["r1"] = map[string]servicenow.GroupToRole{
		"gr1": {BaseResource: servicenow.BaseResource{Id: "gr1"}, Group: "g1", Role: "r1"},
	}
	return snap
}

func TestPruneDeletionsResources(t *testing.T) {
	snap := seedSnapshot()
	pruned, maxTS := pruneDeletions(snap, []servicenow.AuditDeleteRecord{
		del(servicenow.TableUser, "u1", "2026-02-01 00:00:00"),
		del(servicenow.TableUserRole, "r1", "2026-02-02 00:00:00"),
	})
	if pruned != 2 {
		t.Fatalf("expected 2 pruned, got %d", pruned)
	}
	if maxTS != "2026-02-02 00:00:00" {
		t.Fatalf("expected max ts 2026-02-02, got %q", maxTS)
	}
	if _, ok := snap.Users["u1"]; ok {
		t.Fatal("deleted user u1 still present")
	}
	if _, ok := snap.Roles["r1"]; ok {
		t.Fatal("deleted role r1 still present")
	}
	if _, ok := snap.Groups["g1"]; !ok {
		t.Fatal("group g1 should be untouched")
	}
}

func TestPruneDeletionsJoinRows(t *testing.T) {
	snap := seedSnapshot()
	// Delete the membership join row gm1 (NOT user u1), the user-role join ur1,
	// and the group-role join gr1.
	pruned, _ := pruneDeletions(snap, []servicenow.AuditDeleteRecord{
		del(servicenow.TableUserGroupMember, "gm1", "2026-02-01 00:00:00"),
		del(servicenow.TableUserHasRole, "ur1", "2026-02-01 00:00:00"),
		del(servicenow.TableGroupHasRole, "gr1", "2026-02-01 00:00:00"),
	})
	if pruned != 3 {
		t.Fatalf("expected 3 pruned join rows, got %d", pruned)
	}
	if _, ok := snap.GroupMembers["g1"]["gm1"]; ok {
		t.Fatal("deleted membership gm1 still present")
	}
	if _, ok := snap.GroupMembers["g1"]["gm2"]; !ok {
		t.Fatal("sibling membership gm2 was wrongly pruned")
	}
	// UserRoles[r1] and GroupRoles[r1] each had exactly one row; the empty outer
	// bucket should be removed.
	if _, ok := snap.UserRoles["r1"]; ok {
		t.Fatal("emptied UserRoles bucket r1 should be removed")
	}
	if _, ok := snap.GroupRoles["r1"]; ok {
		t.Fatal("emptied GroupRoles bucket r1 should be removed")
	}
	// The user u1 itself must survive: a membership delete is not a user delete.
	if _, ok := snap.Users["u1"]; !ok {
		t.Fatal("user u1 was wrongly pruned by a membership delete")
	}
}

func TestPruneDeletionsTablenameScoping(t *testing.T) {
	// A sys_id that exists as a user must not be pruned by a delete row for a
	// DIFFERENT table that happens to carry the same documentkey.
	snap := newSnapshot("dev1")
	snap.Users["shared"] = user("shared", "2026-01-01 00:00:00")
	pruned, _ := pruneDeletions(snap, []servicenow.AuditDeleteRecord{
		del(servicenow.TableUserGroupMember, "shared", "2026-02-01 00:00:00"),
	})
	if pruned != 0 {
		t.Fatalf("a join-table delete must not prune a user by sys_id, pruned=%d", pruned)
	}
	if _, ok := snap.Users["shared"]; !ok {
		t.Fatal("user 'shared' wrongly pruned by a join-table delete row")
	}
}

func TestReconcilePrunesAndAdvancesDeleteWatermark(t *testing.T) {
	dir := t.TempDir()
	fd := &fakeDeleter{records: []servicenow.AuditDeleteRecord{
		del(servicenow.TableUser, "u1", "2026-03-01 12:00:00"),
		del(servicenow.TableUserGroupMember, "gm1", "2026-03-02 12:00:00"),
	}}
	s, err := Load(dir, "dev1", true, fd)
	if err != nil {
		t.Fatal(err)
	}
	s.snapshot = seedSnapshot()
	s.path = filepath.Join(dir, "baton-servicenow-incremental-dev1.json")

	s.Reconcile(context.Background())
	if fd.calls != 1 {
		t.Fatalf("expected 1 deleter call, got %d", fd.calls)
	}
	if _, ok := s.snapshot.Users["u1"]; ok {
		t.Fatal("u1 not pruned by Reconcile")
	}
	if _, ok := s.snapshot.GroupMembers["g1"]["gm1"]; ok {
		t.Fatal("gm1 not pruned by Reconcile")
	}
	if s.snapshot.DeleteWatermark != "2026-03-02 12:00:00" {
		t.Fatalf("delete watermark not advanced, got %q", s.snapshot.DeleteWatermark)
	}

	// Second call is a no-op (sync.Once): no additional deleter call.
	s.Reconcile(context.Background())
	if fd.calls != 1 {
		t.Fatalf("Reconcile must run once per process, deleter called %d times", fd.calls)
	}

	// The advanced watermark must be persisted and passed back next run.
	s2, err := Load(dir, "dev1", true, fd)
	if err != nil {
		t.Fatal(err)
	}
	if s2.snapshot.DeleteWatermark != "2026-03-02 12:00:00" {
		t.Fatalf("persisted delete watermark wrong, got %q", s2.snapshot.DeleteWatermark)
	}
}

func TestReconcileDegradesGracefullyOnError(t *testing.T) {
	dir := t.TempDir()
	fd := &fakeDeleter{err: errors.New("403 sys_audit_delete not accessible")}
	s, err := Load(dir, "dev1", true, fd)
	if err != nil {
		t.Fatal(err)
	}
	s.snapshot = seedSnapshot()

	// Must not panic, must not prune, must not advance the watermark, must not
	// fail the sync (Reconcile has no error return).
	s.Reconcile(context.Background())
	if _, ok := s.snapshot.Users["u1"]; !ok {
		t.Fatal("audit error must not prune anything")
	}
	if s.snapshot.DeleteWatermark != "" {
		t.Fatalf("audit error must not advance delete watermark, got %q", s.snapshot.DeleteWatermark)
	}
}

func TestWatermarkFrozenWithinRun(t *testing.T) {
	dir := t.TempDir()
	s, err := Load(dir, "dev1", true, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Seed a prior watermark so the read returns a non-empty baseline.
	s.snapshot.Watermarks[StreamGroupMembers] = "2026-01-01 00:00:00"

	first := s.Watermark(StreamGroupMembers)
	if first != "2026-01-01 00:00:00" {
		t.Fatalf("unexpected initial watermark %q", first)
	}
	// A far-future row advances the STORED watermark mid-run (mimics demo data
	// processed for an early group).
	if _, err := s.MergeGroupMembers("g-future", []servicenow.GroupMember{
		{BaseResource: servicenow.BaseResource{Id: "m-future"}, SysUpdatedOn: "2031-01-01 00:00:00"},
	}); err != nil {
		t.Fatal(err)
	}
	// A later group must still read the FROZEN lower bound, not the advanced one,
	// so it does not skip its own rows.
	if again := s.Watermark(StreamGroupMembers); again != first {
		t.Fatalf("watermark must be frozen within a run: got %q want %q", again, first)
	}
}

func TestReconcileNoOpWhenDisabled(t *testing.T) {
	fd := &fakeDeleter{}
	s, err := Load(t.TempDir(), "dev1", false, fd)
	if err != nil {
		t.Fatal(err)
	}
	s.Reconcile(context.Background())
	if fd.calls != 0 {
		t.Fatalf("disabled state must not call the deleter, got %d calls", fd.calls)
	}
}
