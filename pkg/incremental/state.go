// Package incremental implements connector-managed incremental sync for
// baton-servicenow. baton-sdk v0.15.5 has no connector-facing delta/merge, and a
// naive List() filtered by sys_updated_on would drop every unchanged resource
// from the c1z (the SDK does a full replace, not a merge). So the connector
// self-manages a per-deployment on-disk snapshot + watermark: first sync pulls
// everything; later syncs (--incremental) fetch only rows with
// sys_updated_on>=watermark, merge them over the snapshot (upsert by sys_id), and
// emit the union so the c1z stays complete. Strictly opt-in.
//
// Hard deletes don't appear in a sys_updated_on delta; they're reconciled via
// sys_audit_delete (see Reconcile) and by periodic full syncs. Soft-deactivation
// (active=false) is captured because it bumps sys_updated_on.
package incremental

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	"github.com/conductorone/baton-servicenow/pkg/servicenow"
)

// stateVersion is bumped when the on-disk schema changes incompatibly; a
// mismatch forces a full sync. v2 added DeleteWatermark.
const stateVersion = 2

// Deleter fetches hard-delete audit rows; satisfied by *servicenow.Client.
// Injected at Load time to keep this package decoupled from the HTTP client.
type Deleter interface {
	GetAllDeletedSince(ctx context.Context, tableNames []string, createdSince string) ([]servicenow.AuditDeleteRecord, error)
}

// Stream identifies an independently-watermarked record stream, so one stream's
// failure never advances another's past unsynced rows.
type Stream string

const (
	StreamUsers        Stream = "users"
	StreamGroups       Stream = "groups"
	StreamRoles        Stream = "roles"
	StreamGroupMembers Stream = "group_members"
	StreamUserRoles    Stream = "user_roles"
	StreamGroupRoles   Stream = "group_roles"
)

// Snapshot is the full cached result of the last successful sync for one
// ServiceNow deployment. Maps are keyed by sys_id for O(1) upsert/merge.
type Snapshot struct {
	Version    int    `json:"version"`
	Deployment string `json:"deployment"`
	// Watermarks holds the per-stream high-water sys_updated_on (UTC); the next
	// run pulls each stream's rows with sys_updated_on>=Watermarks[stream].
	Watermarks map[Stream]string `json:"watermarks"`

	// DeleteWatermark is the high-water sys_created_on from sys_audit_delete (UTC)
	// across all audited tables; shared because deletions reconcile once per run.
	DeleteWatermark string `json:"delete_watermark"`

	Users  map[string]servicenow.User  `json:"users"`
	Roles  map[string]servicenow.Role  `json:"roles"`
	Groups map[string]servicenow.Group `json:"groups"`

	GroupMembers map[string]map[string]servicenow.GroupMember `json:"group_members"` // group sys_id -> row sys_id -> row
	UserRoles    map[string]map[string]servicenow.UserToRole  `json:"user_roles"`    // role sys_id -> row sys_id -> row
	GroupRoles   map[string]map[string]servicenow.GroupToRole `json:"group_roles"`   // role sys_id -> row sys_id -> row
}

func newSnapshot(deployment string) *Snapshot {
	return &Snapshot{
		Version:      stateVersion,
		Deployment:   deployment,
		Watermarks:   map[Stream]string{},
		Users:        map[string]servicenow.User{},
		Roles:        map[string]servicenow.Role{},
		Groups:       map[string]servicenow.Group{},
		GroupMembers: map[string]map[string]servicenow.GroupMember{},
		UserRoles:    map[string]map[string]servicenow.UserToRole{},
		GroupRoles:   map[string]map[string]servicenow.GroupToRole{},
	}
}

// State is a thread-safe handle to one deployment's incremental snapshot, backed
// by a JSON file. The zero value is not usable; construct with Load. There is no
// reliable end-of-sync hook, so each Merge* persists immediately and advances
// only its own stream's watermark.
type State struct {
	mu       sync.Mutex
	path     string
	enabled  bool
	snapshot *Snapshot
	// failed blocks all subsequent persistence so a partial run never advances
	// watermarks past unsynced rows.
	failed bool

	deleter       Deleter   // fetches sys_audit_delete rows; nil disables deletion capture
	reconcileOnce sync.Once // deletion reconcile runs exactly once per run

	// readWatermarks freezes each stream's FETCH watermark for the run: without
	// it, a far-future row (e.g. demo data) merged early would advance the
	// watermark past groups/roles processed later, dropping their rows.
	readWatermarks map[Stream]string
}

// Load opens (or initializes) the incremental state for a deployment. When
// disabled the returned State is a no-op (full pull, no persistence). A missing,
// unreadable, or version-mismatched snapshot yields an empty snapshot (next sync
// is a full pull that seeds the cache) rather than an error.
func Load(dir string, deployment string, enabled bool, deleter Deleter) (*State, error) {
	if !enabled {
		return &State{enabled: false, snapshot: newSnapshot(deployment)}, nil
	}
	if deployment == "" {
		return nil, fmt.Errorf("baton-servicenow: incremental state requires a deployment")
	}
	if dir == "" {
		var err error
		dir, err = os.Getwd()
		if err != nil {
			return nil, fmt.Errorf("baton-servicenow: resolving incremental state dir: %w", err)
		}
	}
	path := filepath.Join(dir, fmt.Sprintf("baton-servicenow-incremental-%s.json", sanitize(deployment)))

	s := &State{path: path, enabled: true, snapshot: newSnapshot(deployment), deleter: deleter}

	data, err := os.ReadFile(path)
	switch {
	case os.IsNotExist(err):
		return s, nil // first run: empty snapshot, full pull
	case err != nil:
		return nil, fmt.Errorf("baton-servicenow: reading incremental state %q: %w", path, err)
	}

	var snap Snapshot
	if jErr := json.Unmarshal(data, &snap); jErr != nil || snap.Version != stateVersion || snap.Deployment != deployment {
		return s, nil //nolint:nilerr // corrupt/incompatible/wrong-deployment snapshot is non-fatal: discard and do a full pull
	}
	ensureMaps(&snap)
	s.snapshot = &snap
	return s, nil
}

// Enabled reports whether incremental mode is on.
func (s *State) Enabled() bool { return s != nil && s.enabled }

// MarkFailed records that the run hit an error, making all subsequent
// persistence a no-op. Call before returning any error from a syncer.
func (s *State) MarkFailed() {
	if !s.Enabled() {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failed = true
}

// Watermark returns the sys_updated_on lower bound for a stream (empty = full
// pull), frozen at the first read per run so every per-group/per-role fetch uses
// the same bound.
func (s *State) Watermark(stream Stream) string {
	if !s.Enabled() {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.readWatermarks == nil {
		s.readWatermarks = map[Stream]string{}
	}
	if wm, ok := s.readWatermarks[stream]; ok {
		return wm
	}
	wm := s.snapshot.Watermarks[stream]
	s.readWatermarks[stream] = wm
	return wm
}

// advance bumps a stream's watermark to ts if greater, but never past now():
// a future-dated sys_updated_on (clock skew, demo data) would otherwise push the
// watermark beyond real changes and stall detection until a full sync. Lexical
// compare is valid for the fixed-width UTC string. Caller must hold s.mu.
func (s *State) advance(stream Stream, ts string) {
	if ts == "" || ts > nowWatermark() {
		return
	}
	if ts > s.snapshot.Watermarks[stream] {
		s.snapshot.Watermarks[stream] = ts
	}
}

// nowWatermark is the current time as a ServiceNow UTC "YYYY-MM-DD HH:MM:SS"
// string, for lexical comparison against sys_updated_on / sys_created_on.
func nowWatermark() string {
	return time.Now().UTC().Format("2006-01-02 15:04:05")
}

// Reconcile captures hard deletes once per run: it fetches sys_audit_delete rows
// at or after the stored delete watermark, prunes the matching sys_ids from the
// snapshot, advances the watermark, and persists. If the audit table is
// unqueryable it logs and swallows (the full-sync backstop covers deletions);
// it never fails the sync. No-op when disabled, after MarkFailed, or no deleter.
func (s *State) Reconcile(ctx context.Context) {
	if !s.Enabled() || s.deleter == nil {
		return
	}
	s.reconcileOnce.Do(func() {
		l := ctxzap.Extract(ctx)

		s.mu.Lock()
		since := s.snapshot.DeleteWatermark
		s.mu.Unlock()

		records, err := s.deleter.GetAllDeletedSince(ctx, servicenow.AuditedTables, since)
		if err != nil {
			l.Warn("baton-servicenow: deletion capture unavailable, skipping (full sync will reconcile)",
				zap.String("delete_watermark", since),
				zap.Error(err),
			)
			return
		}

		s.mu.Lock()
		defer s.mu.Unlock()
		pruned, maxTS := pruneDeletions(s.snapshot, records)
		// Cap at now() (as in advance) so a future-dated audit row can't push past real deletes.
		if maxTS != "" && maxTS <= nowWatermark() && maxTS > s.snapshot.DeleteWatermark {
			s.snapshot.DeleteWatermark = maxTS
		}
		if err := s.persist(); err != nil {
			l.Warn("baton-servicenow: failed to persist state after deletion reconcile", zap.Error(err))
			return
		}
		l.Debug("baton-servicenow: deletion reconcile complete",
			zap.Int("audit_rows", len(records)),
			zap.Int("pruned", pruned),
			zap.String("delete_watermark", s.snapshot.DeleteWatermark),
		)
	})
}

// pruneDeletions removes every deleted DocumentKey from the snapshot, returning
// the count pruned and the max sys_created_on seen. The lookup is keyed by
// Tablename so a sys_id collision across tables can't prune the wrong record.
// Pure function over *Snapshot for unit-testing.
func pruneDeletions(snap *Snapshot, records []servicenow.AuditDeleteRecord) (int, string) {
	pruned := 0
	maxTS := ""
	for _, rec := range records {
		if rec.SysCreatedOn > maxTS {
			maxTS = rec.SysCreatedOn
		}
		key := rec.DocumentKey
		if key == "" {
			continue
		}
		switch rec.Tablename {
		case servicenow.TableUser:
			if _, ok := snap.Users[key]; ok {
				delete(snap.Users, key)
				pruned++
			}
		case servicenow.TableUserGroup:
			if _, ok := snap.Groups[key]; ok {
				delete(snap.Groups, key)
				pruned++
			}
			delete(snap.GroupMembers, key) // drop the deleted group's membership rows too
		case servicenow.TableUserRole:
			if _, ok := snap.Roles[key]; ok {
				delete(snap.Roles, key)
				pruned++
			}
		case servicenow.TableUserGroupMember:
			if pruneNested(snap.GroupMembers, key) {
				pruned++
			}
		case servicenow.TableUserHasRole:
			if pruneNested(snap.UserRoles, key) {
				pruned++
			}
		case servicenow.TableGroupHasRole:
			if pruneNested(snap.GroupRoles, key) {
				pruned++
			}
		}
	}
	return pruned, maxTS
}

// pruneNested removes a join-row sys_id from whichever outer bucket holds it,
// returning true if removed.
func pruneNested[T any](m map[string]map[string]T, rowID string) bool {
	for outer, inner := range m {
		if _, ok := inner[rowID]; ok {
			delete(inner, rowID)
			if len(inner) == 0 {
				delete(m, outer)
			}
			return true
		}
	}
	return false
}

// persist marshals and atomically writes the snapshot. Caller must hold s.mu.
// A no-op once the run has been marked failed.
func (s *State) persist() error {
	if s.failed {
		return nil
	}
	data, err := json.MarshalIndent(s.snapshot, "", "  ")
	if err != nil {
		return fmt.Errorf("baton-servicenow: marshaling incremental state: %w", err)
	}
	// Unique temp file + rename: concurrent writers sharing the state path would
	// collide on a fixed "<path>.tmp" (one's rename hits a temp already renamed away).
	dir := filepath.Dir(s.path)
	tmpFile, err := os.CreateTemp(dir, filepath.Base(s.path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("baton-servicenow: creating temp incremental state in %q: %w", dir, err)
	}
	tmp := tmpFile.Name()
	if _, err := tmpFile.Write(data); err != nil {
		tmpFile.Close()
		os.Remove(tmp)
		return fmt.Errorf("baton-servicenow: writing incremental state %q: %w", tmp, err)
	}
	if err := tmpFile.Close(); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("baton-servicenow: closing incremental state %q: %w", tmp, err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		os.Remove(tmp)
		return fmt.Errorf("baton-servicenow: committing incremental state %q: %w", s.path, err)
	}
	return nil
}

// MergeUsers upserts changed users, advances the watermark, persists, and
// returns the full merged set (snapshot ∪ changed). No-op storage when disabled.
func (s *State) MergeUsers(changed []servicenow.User) ([]servicenow.User, error) {
	if !s.Enabled() {
		return changed, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, u := range changed {
		s.snapshot.Users[u.Id] = u
		s.advance(StreamUsers, u.SysUpdatedOn)
	}
	out := make([]servicenow.User, 0, len(s.snapshot.Users))
	for _, u := range s.snapshot.Users {
		out = append(out, u)
	}
	return out, s.persist()
}

// MergeRoles upserts changed roles and returns the full merged set.
func (s *State) MergeRoles(changed []servicenow.Role) ([]servicenow.Role, error) {
	if !s.Enabled() {
		return changed, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range changed {
		s.snapshot.Roles[r.Id] = r
		s.advance(StreamRoles, r.SysUpdatedOn)
	}
	out := make([]servicenow.Role, 0, len(s.snapshot.Roles))
	for _, r := range s.snapshot.Roles {
		out = append(out, r)
	}
	return out, s.persist()
}

// MergeGroups upserts changed groups and returns the full merged set.
func (s *State) MergeGroups(changed []servicenow.Group) ([]servicenow.Group, error) {
	if !s.Enabled() {
		return changed, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, g := range changed {
		s.snapshot.Groups[g.Id] = g
		s.advance(StreamGroups, g.SysUpdatedOn)
	}
	out := make([]servicenow.Group, 0, len(s.snapshot.Groups))
	for _, g := range s.snapshot.Groups {
		out = append(out, g)
	}
	return out, s.persist()
}

// MergeGroupMembers upserts changed membership rows for one group, advances the
// (group-shared) watermark, persists, and returns that group's full merged set.
func (s *State) MergeGroupMembers(groupID string, changed []servicenow.GroupMember) ([]servicenow.GroupMember, error) {
	if !s.Enabled() {
		return changed, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.snapshot.GroupMembers[groupID]
	if cur == nil {
		cur = map[string]servicenow.GroupMember{}
		s.snapshot.GroupMembers[groupID] = cur
	}
	for _, m := range changed {
		cur[m.Id] = m
		s.advance(StreamGroupMembers, m.SysUpdatedOn)
	}
	out := make([]servicenow.GroupMember, 0, len(cur))
	for _, m := range cur {
		out = append(out, m)
	}
	return out, s.persist()
}

// MergeUserRoles upserts changed user-role rows for roleID and returns its full merged set.
func (s *State) MergeUserRoles(roleID string, changed []servicenow.UserToRole) ([]servicenow.UserToRole, error) {
	if !s.Enabled() {
		return changed, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.snapshot.UserRoles[roleID]
	if cur == nil {
		cur = map[string]servicenow.UserToRole{}
		s.snapshot.UserRoles[roleID] = cur
	}
	for _, r := range changed {
		cur[r.Id] = r
		s.advance(StreamUserRoles, r.SysUpdatedOn)
	}
	out := make([]servicenow.UserToRole, 0, len(cur))
	for _, r := range cur {
		out = append(out, r)
	}
	return out, s.persist()
}

// MergeGroupRoles upserts changed group-role rows for roleID and returns its full merged set.
func (s *State) MergeGroupRoles(roleID string, changed []servicenow.GroupToRole) ([]servicenow.GroupToRole, error) {
	if !s.Enabled() {
		return changed, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.snapshot.GroupRoles[roleID]
	if cur == nil {
		cur = map[string]servicenow.GroupToRole{}
		s.snapshot.GroupRoles[roleID] = cur
	}
	for _, r := range changed {
		cur[r.Id] = r
		s.advance(StreamGroupRoles, r.SysUpdatedOn)
	}
	out := make([]servicenow.GroupToRole, 0, len(cur))
	for _, r := range cur {
		out = append(out, r)
	}
	return out, s.persist()
}

// Save is a best-effort final flush (if a Close hook fires); persistence
// normally happens inline in each Merge*. No-op when disabled or after failure.
func (s *State) Save() error {
	if !s.Enabled() {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.persist()
}

func ensureMaps(s *Snapshot) {
	if s.Users == nil {
		s.Users = map[string]servicenow.User{}
	}
	if s.Roles == nil {
		s.Roles = map[string]servicenow.Role{}
	}
	if s.Groups == nil {
		s.Groups = map[string]servicenow.Group{}
	}
	if s.GroupMembers == nil {
		s.GroupMembers = map[string]map[string]servicenow.GroupMember{}
	}
	if s.UserRoles == nil {
		s.UserRoles = map[string]map[string]servicenow.UserToRole{}
	}
	if s.GroupRoles == nil {
		s.GroupRoles = map[string]map[string]servicenow.GroupToRole{}
	}
}

// sanitize makes a deployment name safe for use in a filename.
func sanitize(s string) string {
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			out = append(out, r)
		default:
			out = append(out, '_')
		}
	}
	return string(out)
}
