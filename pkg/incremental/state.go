// Package incremental implements connector-managed incremental sync state for
// baton-servicenow.
//
// # Why this exists (the SDK constraint)
//
// baton-sdk v0.15.5 provides NO connector-facing delta/merge mechanism:
//   - "Partial" syncs (connectorstore.SyncTypePartial) are TARGETED-RESOURCE
//     syncs, not time-windowed deltas.
//   - The session store (pkg/types/sessions) is keyed by sync_id, so anything
//     written under one sync is invisible to the next — it cannot hold a
//     cross-sync watermark.
//   - ETag-based grant replay (pkg/sync/syncer.go syncGrantsForResource) does
//     NOT inspect/replay prior ETags in this version: it always writes exactly
//     the grants the connector returns. previousSyncReader is opened and closed
//     but never read. The ETag annotation protos are tombstoned/deprecated.
//
// Consequence: if a connector simply filtered List() by sys_updated_on>=last,
// every UNCHANGED resource would be dropped from the c1z (the SDK does a full
// replace, not a merge), corrupting the access graph.
//
// # What this package does
//
// It makes the connector self-manage the watermark AND the full prior snapshot
// on local disk, keyed by ServiceNow deployment:
//
//	First sync (no state, or --incremental off): full pull, snapshot persisted.
//	Later syncs (--incremental on): fetch only rows with sys_updated_on>=watermark,
//	  MERGE them over the cached snapshot (upsert by sys_id), emit the UNION so
//	  the c1z stays complete, then persist the new snapshot + advanced watermark.
//
// This keeps the c1z correct while only paying the API cost of changed rows.
// It is strictly opt-in; with incremental disabled the connector behaves
// exactly as before.
//
// # Known limitations
//   - Deletions: rows hard-deleted in ServiceNow do not appear in a
//     sys_updated_on delta, so they linger in the snapshot until a full sync.
//     Run a periodic full sync (incremental off) to reconcile. Soft-deactivation
//     (active=false) IS captured because it bumps sys_updated_on.
//   - The watermark is advanced to the max sys_updated_on actually observed in a
//     run (never to "now"), so rows written during the sync window are caught
//     next time. The >= boundary may re-fetch same-second rows; merge is
//     idempotent (upsert by sys_id) so that is harmless.
package incremental

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/conductorone/baton-servicenow/pkg/servicenow"
)

// stateVersion is bumped when the on-disk schema changes incompatibly; a
// mismatch is treated as "no usable state" and forces a full sync.
const stateVersion = 1

// Stream identifies an independently-watermarked record stream. Each stream
// advances its own watermark so a failure in one (which prevents its Save)
// never advances another's past unsynced rows. Re-fetching at the >= boundary
// is idempotent (upsert by sys_id), so a slightly stale per-stream watermark is
// always safe.
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
	// Watermarks holds the per-stream high-water sys_updated_on
	// ("YYYY-MM-DD HH:MM:SS", UTC). The next run pulls each stream's rows with
	// sys_updated_on>=Watermarks[stream].
	Watermarks map[Stream]string `json:"watermarks"`

	Users  map[string]servicenow.User  `json:"users"`
	Roles  map[string]servicenow.Role  `json:"roles"`
	Groups map[string]servicenow.Group `json:"groups"`

	// GroupMembers: group sys_id -> (membership row sys_id -> row).
	GroupMembers map[string]map[string]servicenow.GroupMember `json:"group_members"`
	// UserRoles: user sys_id -> (row sys_id -> row).
	UserRoles map[string]map[string]servicenow.UserToRole `json:"user_roles"`
	// GroupRoles: group sys_id -> (row sys_id -> row).
	GroupRoles map[string]map[string]servicenow.GroupToRole `json:"group_roles"`
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

// State is a thread-safe handle to one deployment's incremental snapshot,
// backed by a JSON file. The zero value is not usable; construct with Load.
//
// Persistence model: there is no reliable end-of-sync hook for an in-process
// c1z sync (the SDK's connector wrapper does not forward Close to the connector
// implementation), so each Merge* call persists the snapshot to disk
// immediately and advances ONLY its own stream's watermark. A failed stream
// calls MarkFailed before returning, which blocks its own Save, leaving that
// stream's watermark untouched for the next run. Per-stream watermarks mean one
// stream's failure never advances another's past unsynced rows.
type State struct {
	mu       sync.Mutex
	path     string
	enabled  bool
	snapshot *Snapshot
	// failed is set if a syncer reported an error this run, blocking subsequent
	// persistence so a partial run never advances watermarks past unsynced rows.
	failed bool
}

// Load opens (or initializes) the incremental state for a deployment.
//
// When enabled is false, the returned State is a no-op: Watermark returns ""
// (full pull), merges are skipped, and Save does nothing — so the connector
// behaves exactly as a non-incremental connector.
//
// When enabled is true, the snapshot is read from
// <dir>/baton-servicenow-incremental-<deployment>.json. A missing, unreadable,
// or version-mismatched file yields an empty snapshot (the next sync is a full
// pull that seeds the cache) rather than an error, so a bad cache never fails
// a sync.
func Load(dir string, deployment string, enabled bool) (*State, error) {
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

	s := &State{path: path, enabled: true, snapshot: newSnapshot(deployment)}

	data, err := os.ReadFile(path)
	switch {
	case os.IsNotExist(err):
		return s, nil // first run: empty snapshot, full pull
	case err != nil:
		return nil, fmt.Errorf("baton-servicenow: reading incremental state %q: %w", path, err)
	}

	var snap Snapshot
	if jErr := json.Unmarshal(data, &snap); jErr != nil || snap.Version != stateVersion || snap.Deployment != deployment {
		// Corrupt / incompatible / wrong-deployment cache: discard, full pull.
		return s, nil
	}
	ensureMaps(&snap)
	s.snapshot = &snap
	return s, nil
}

// Enabled reports whether incremental mode is on.
func (s *State) Enabled() bool { return s != nil && s.enabled }

// MarkFailed records that the run hit an error. All subsequent persistence
// (inline Merge* writes and Save) becomes a no-op, so no stream's watermark is
// advanced past data that may not have synced. Call it before returning any
// error from a syncer.
func (s *State) MarkFailed() {
	if !s.Enabled() {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failed = true
}

// Watermark returns the sys_updated_on lower bound for a stream, to pass to the
// matching *UpdatedSince client method. Empty string means "full pull" (first
// sync, disabled mode, or a stream that has not yet completed once).
func (s *State) Watermark(stream Stream) string {
	if !s.Enabled() {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.snapshot.Watermarks[stream]
}

// advance bumps a stream's watermark to ts if ts is greater. Lexical comparison
// is valid because sys_updated_on is a fixed-width "YYYY-MM-DD HH:MM:SS" UTC
// string. Caller must hold s.mu.
func (s *State) advance(stream Stream, ts string) {
	if ts > s.snapshot.Watermarks[stream] {
		s.snapshot.Watermarks[stream] = ts
	}
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
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return fmt.Errorf("baton-servicenow: writing incremental state %q: %w", tmp, err)
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("baton-servicenow: committing incremental state %q: %w", s.path, err)
	}
	return nil
}

// MergeUsers upserts the changed users, advances the users watermark, persists,
// and returns the full merged set (snapshot ∪ changed). In non-incremental mode
// it returns changed unchanged and stores nothing.
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

// MergeRoles upserts changed roles, advances the roles watermark, persists, and
// returns the full merged set.
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

// MergeGroups upserts changed groups, advances the groups watermark, persists,
// and returns the full merged set.
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

// MergeGroupMembers upserts changed membership rows for a single group, advances
// the group-members watermark, persists, and returns the full merged set for
// that group.
//
// The group-members watermark is shared across all groups: rows are pulled
// per-group with the same lower bound, and each row carries its own
// sys_updated_on, so the global max is a safe lower bound for every group next
// run.
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

// MergeUserRoles upserts changed user-role rows keyed by role, advances the
// user-roles watermark, persists, and returns the full merged set for roleID.
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

// MergeGroupRoles upserts changed group-role rows keyed by role, advances the
// group-roles watermark, persists, and returns the full merged set for roleID.
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

// Save is retained as a final best-effort flush (e.g. if a Close hook does fire
// in plugin/server mode). It is a no-op when disabled or after a failure.
// Persistence normally happens inline in each Merge*; callers need not rely on
// Save being invoked.
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
