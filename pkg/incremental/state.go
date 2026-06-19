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
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	"github.com/conductorone/baton-servicenow/pkg/servicenow"
)

// stateVersion is bumped when the on-disk schema changes incompatibly; a
// mismatch is treated as "no usable state" and forces a full sync.
//
// v2 added the DeleteWatermark field (deletion capture via sys_audit_delete).
// A v1 file unmarshals fine into the v2 struct, but the version check forces a
// full pull on upgrade so the delete watermark starts from a known-good
// snapshot rather than mid-history.
const stateVersion = 2

// Deleter fetches hard-delete audit rows. It is satisfied by *servicenow.Client
// (GetAllDeletedSince). It is injected at Load time so the incremental package
// stays decoupled from the HTTP client and is easy to fake in tests.
type Deleter interface {
	GetAllDeletedSince(ctx context.Context, tableNames []string, createdSince string) ([]servicenow.AuditDeleteRecord, error)
}

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

	// DeleteWatermark is the high-water sys_created_on from sys_audit_delete
	// ("YYYY-MM-DD HH:MM:SS", UTC) across ALL audited connector tables. The next
	// run fetches delete-audit rows with sys_created_on>=DeleteWatermark and
	// prunes the matching sys_ids from the snapshot. A single shared watermark is
	// safe because deletions are reconciled once per run over all tables together.
	DeleteWatermark string `json:"delete_watermark"`

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

	// deleter fetches sys_audit_delete rows; nil disables deletion capture.
	deleter Deleter
	// reconcileOnce guards the single per-run deletion reconciliation so it runs
	// exactly once no matter which syncer (users/groups/roles) reaches it first.
	reconcileOnce sync.Once

	// readWatermarks freezes the watermark each stream uses for FETCHING for the
	// duration of a run. Without this, group-members/user-roles/group-roles —
	// which are fetched per group/role across many parallel calls within one sync
	// — would read a watermark that Merge* is concurrently advancing, so a row
	// with a far-future sys_updated_on (e.g. demo data) processed early would push
	// the watermark past rows of groups/roles processed later, dropping them. The
	// frozen value is captured the first time a stream's watermark is read this
	// run and never moves again until the next process.
	readWatermarks map[Stream]string
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
//
// The value is FROZEN at the first read per run (see readWatermarks): every
// per-group / per-role fetch in one sync uses the same lower bound, so a
// concurrently-advancing stored watermark (driven by far-future demo timestamps)
// cannot cause later groups/roles to skip their rows.
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

// advance bumps a stream's watermark to ts if ts is greater. Lexical comparison
// is valid because sys_updated_on is a fixed-width "YYYY-MM-DD HH:MM:SS" UTC
// string. Caller must hold s.mu.
func (s *State) advance(stream Stream, ts string) {
	if ts > s.snapshot.Watermarks[stream] {
		s.snapshot.Watermarks[stream] = ts
	}
}

// Reconcile captures hard deletes once per run. It fetches sys_audit_delete
// rows for every audited connector table logged at or after the stored delete
// watermark, prunes the matching sys_ids out of the snapshot (resources by
// sys_id; join rows out of the nested membership/assignment maps), advances the
// delete watermark to the max sys_created_on observed, and persists.
//
// It runs at most once per process (sync.Once), so whichever syncer hits its
// incremental branch first triggers it; the prune mutates the shared snapshot
// maps in place, so every later Merge* emits a union that already excludes the
// deleted rows. The reconciliation point is therefore "at sync start, before any
// union is built", as required.
//
// Graceful degradation: if the audit table is unqueryable (auditing disabled,
// no read access, any 4xx/5xx) the error is logged and swallowed — deletions
// are simply not captured this run and the periodic full-sync backstop covers
// them. A failed reconcile NEVER fails the sync and NEVER advances the delete
// watermark. It is a no-op when disabled, after MarkFailed, or with no deleter.
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
			// Degrade gracefully: deletions not captured this run; the full-sync
			// backstop reconciles them. Do not fail the sync.
			l.Warn("baton-servicenow: deletion capture unavailable, skipping (full sync will reconcile)",
				zap.String("delete_watermark", since),
				zap.Error(err),
			)
			return
		}

		s.mu.Lock()
		defer s.mu.Unlock()
		pruned, maxTS := pruneDeletions(s.snapshot, records)
		if maxTS > s.snapshot.DeleteWatermark {
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

// pruneDeletions removes every deleted sys_id (records[i].DocumentKey) from the
// snapshot and returns the count pruned and the max sys_created_on seen. A
// DocumentKey may be a resource sys_id (sys_user/sys_user_group/sys_user_role)
// or a join-row sys_id (sys_user_grmember/sys_user_has_role/sys_group_has_role);
// for join rows it is removed from whichever nested map contains it. The lookup
// is driven by Tablename so a sys_id collision across tables cannot prune the
// wrong record. Caller must hold the snapshot's lock (or own the snapshot).
//
// Split out as a pure function over *Snapshot so the prune logic is unit-tested
// without HTTP/IO.
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
			// A deleted group's membership/role rows may linger; drop the nested
			// maps keyed by this group too so its grants disappear.
			delete(snap.GroupMembers, key)
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

// pruneNested removes a join-row sys_id from whichever outer bucket
// (group/role) holds it in a snapshot nested map. Returns true if removed.
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
	// Write to a UNIQUE temp file (not a fixed "<path>.tmp") then rename. The
	// baton-sdk may run the connector across multiple goroutines/instances that
	// share the same state path; a fixed temp name lets two writers collide so
	// one's rename hits a temp the other already renamed away (ENOENT). A unique
	// temp per write keeps each rename atomic and independent.
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
