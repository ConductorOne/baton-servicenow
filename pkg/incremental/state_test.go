package incremental

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/conductorone/baton-servicenow/pkg/servicenow"
)

func user(id, updated string) servicenow.User {
	u := servicenow.User{SysUpdatedOn: updated}
	u.Id = id
	return u
}

func TestDisabledIsNoOp(t *testing.T) {
	s, err := Load(t.TempDir(), "dev1", false)
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
	s, err := Load(dir, "dev1", true)
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
	s2, err := Load(dir, "dev1", true)
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

	s3, err := Load(dir, "dev1", true)
	if err != nil {
		t.Fatal(err)
	}
	if got := s3.Watermark(StreamUsers); got != "2026-01-06 08:00:00" {
		t.Fatalf("watermark not advanced after delta, got %q", got)
	}
}

func TestFailedRunDoesNotPersist(t *testing.T) {
	dir := t.TempDir()
	s, err := Load(dir, "dev1", true)
	if err != nil {
		t.Fatal(err)
	}
	// MarkFailed before the merge: the merge's inline persist must be a no-op.
	s.MarkFailed()
	if _, err := s.MergeUsers([]servicenow.User{user("a", "2026-01-01 10:00:00")}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "baton-servicenow-incremental-dev1.json")
	s2, _ := Load(dir, "dev1", true)
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
	s, err := Load(dir, "dev1", true)
	if err != nil {
		t.Fatalf("corrupt cache must not error, got %v", err)
	}
	if s.Watermark(StreamUsers) != "" {
		t.Fatal("corrupt cache should yield empty watermark (full pull)")
	}
}

func TestGroupMemberMergePerGroup(t *testing.T) {
	dir := t.TempDir()
	s, _ := Load(dir, "dev1", true)

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
