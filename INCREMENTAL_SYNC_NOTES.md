# baton-servicenow incremental sync — design & implementation notes

Branch: `feat/servicenow-incremental-sync` (off `origin/main`).
baton-sdk: **v0.15.5** (latest at time of writing).

## TL;DR

Incremental sync is implemented as a **strictly opt-in, connector-managed
watermark + cached-snapshot + merge** mechanism, gated by `--incremental`
(default off). With it off, the connector behaves exactly as before.

The reason it is connector-managed (and not a thin "filter List by
sys_updated_on") is a hard SDK constraint discovered during research: **baton-sdk
v0.15.5 has no working connector-facing delta/merge.** A naive list-filter would
permanently drop unchanged resources from each sync's c1z, corrupting the access
graph. So the connector keeps its own prior snapshot on disk, fetches only
changed rows, merges them over the snapshot, and emits the complete union — the
c1z stays whole while the API only pays for changed rows.

---

## 1. SDK incremental-sync mechanism — what actually exists in v0.15.5

Confirmed by reading the SDK source at
`$(go env GOMODCACHE)/github.com/conductorone/baton-sdk@v0.15.5`:

- **No SDK-orchestrated time-windowed delta + merge.** The SDK's `SyncType`
  values (`pkg/connectorstore/connectorstore.go:11-29`) are `full`, `partial`,
  `resources_only`, `partial_upserts`, `partial_deletions`. "partial" means a
  **targeted-resource** sync (sync specific named resources), started only when
  `targetedSyncResources` is non-empty (`pkg/sync/syncer.go:345-390`). It is NOT
  a "records changed since time T" delta.

- **The session store is per-sync, not durable.** `SyncOpAttrs{Session, SyncID,
  PageToken}` (`pkg/types/resource/resource.go:485-489`) carries a
  `sessions.SessionStore`, but the backing `C1File` store has a UNIQUE index on
  `(sync_id, key)` and every Get/Set filters `where sync_id = ?`
  (`pkg/dotc1z/session_store.go`). Data written under sync N is invisible to sync
  N+1. It cannot hold a cross-sync watermark.

- **ETag grant-replay is dormant in this version.** The `ETag`/`ETagMatch`
  annotation protos are tombstoned/deprecated
  (`proto/c1/connector/v2/annotation_etag.proto`: "not honored by the SDK …
  removed when etag-based grant replay was removed (PR #951)"). The live grant
  path `syncGrantsForResource` (`pkg/sync/syncer.go:1692-1819`) calls
  `ListGrants` and writes exactly the returned grants via `PutGrants` — it never
  inspects a prior ETag, never matches, never replays. `WithPreviousSyncC1ZPath`
  opens `previousSyncReader` but the reader is only Closed, never read during the
  sync loop. (`pebble_etag_replay_test.go` exercises a lower-level store path
  that the syncer does not wire up.)

- **No connector annotation to opt into incremental.** None exists.

- **baton-servicenow uses the V1 `ResourceSyncer` interface**
  (`List/Entitlements/Grants(ctx, …, *pagination.Token) ([]…, string,
  annotations.Annotations, error)` — `pkg/connectorbuilder/resource_syncer.go:35`).
  The V1→V2 adapter discards `SyncID`/`Session`
  (`resource_syncer.go:353-395`), so even V2 wouldn't help: the session store is
  per-sync regardless.

**Conclusion:** the watermark must be connector-self-managed AND the connector
must carry forward unchanged data itself, because the SDK will not merge.

Cross-checked by an independent source trace; both reached the same conclusion.

## 2. Reference connectors

A `gh search code --owner conductorone` sweep for ETag/previous-sync/watermark
usage found **no baton connector that implements a `sys_updated_on`-style
time-watermark incremental sync** on this SDK line — the only hits for
`WithPreviousSyncC1ZPath` are in baton-sdk itself (the dormant replay path). The
implemented pattern here is therefore a first-of-kind for this repo, grounded in
the SDK contract above rather than copied from a peer connector.

## 3. ServiceNow query syntax (verified live against `dev289997`)

- Filter form: `sysparm_query=...^sys_updated_on>=YYYY-MM-DD HH:MM:SS`
  (`^` = AND). Verified:
  - `>=2026-05-17 23:34:46` → X-Total-Count 637 (all users);
    `>=2026-05-17 23:34:47` → 589. Precise `>=` boundary semantics confirmed.
  - A future `>=` returns 0 rows (correct filtering).
- **Timezone:** the integration account's session is **UTC** — a record created
  at UTC `02:42:04` got `sys_updated_on = 02:42:05`. So storing/comparing the
  watermark as a UTC `YYYY-MM-DD HH:MM:SS` string is correct for this account.
  (If a deployment's service account is non-UTC, the watermark would need TZ
  normalization; documented as an open item.)
- Lexical string comparison of `sys_updated_on` is valid for advancing the
  watermark because the format is fixed-width.

## 4. Implementation — file by file

### `pkg/servicenow/model.go`
Added `SysUpdatedOn string json:"sys_updated_on"` to `User`, `Role`, `Group`,
`GroupMember`, `UserToRole`, `GroupToRole` so every row carries its watermark.

### `pkg/servicenow/request.go`
- Added `sys_updated_on` to `UserFields`, `RoleFields`, `GroupFields` and to the
  membership filter field lists.
- `UpdatedSinceField` const + `appendUpdatedSince(query, ts)` helper that ANDs a
  `sys_updated_on>=ts` clause onto an existing query (empty ts = no-op).

### `pkg/servicenow/client.go`
- For each list method, added a `…UpdatedSince(…, updatedSince string)` variant;
  the original method now delegates with `""` (full pull) — backward compatible.
  Covers: `GetUsersUpdatedSince`, `GetGroupsUpdatedSince`, `GetRolesUpdatedSince`,
  `GetUserToGroupUpdatedSince`, `GetUserToRoleUpdatedSince`,
  `GetGroupToRoleUpdatedSince`.
- `drainAll[T]` generic + `GetAll…UpdatedSince` helpers that page through ALL
  delta rows in one call (incremental emits the full union as a single page, so
  it must drain internally). Includes a non-advancing-token guard.

### Deletion capture (`sys_audit_delete`) — closes the deletion gap

`sys_updated_on` polling detects inserts/updates but NOT hard deletes (a deleted
row never gets a newer timestamp), so deleted users/groups/roles/memberships
would linger in the snapshot and emit stale grants forever. Deletion capture
reconciles those out by reading ServiceNow's **`sys_audit_delete`** table.

- `model.go`: `AuditDeleteRecord{Tablename, DocumentKey, SysCreatedOn}`, the
  audited table-name constants (`TableUser`, `TableUserGroup`, `TableUserRole`,
  `TableUserGroupMember`, `TableUserHasRole`, `TableGroupHasRole`), and
  `AuditedTables` (all six). Verified live: every connector table is
  delete-audited on `dev289997`.
- `client.go`: `GetDeletedSince(ctx, tableNames, createdSince, pv)` queries
  `sys_audit_delete` with `tablename=<t>` (or `tablenameIN<t1>,<t2>`)
  `^sys_created_on>=<watermark>` and `sysparm_exclude_reference_link=true` (the
  client default). `GetAllDeletedSince` drains all pages.
- `incremental/state.go`:
  - `Snapshot.DeleteWatermark` (JSON `delete_watermark`) — a SINGLE shared
    high-water `sys_created_on` across all audited tables, persisted alongside
    the per-stream update watermarks. Separate from them by design: deletions
    are reconciled once per run over all tables together.
  - `Deleter` interface (satisfied by `*servicenow.Client`) injected at `Load`,
    so the package stays decoupled from HTTP and is trivially faked in tests.
  - `Reconcile(ctx)` runs **once per process** (`sync.Once`): fetches every
    audit-delete row `>= DeleteWatermark`, calls `pruneDeletions`, advances the
    delete watermark to the max `sys_created_on` seen, and persists. It is
    triggered at the top of each syncer's incremental branch, so whichever of
    users/groups/roles runs first reconciles **before any merged union is
    built** — the prune mutates the shared snapshot maps in place, so every later
    `Merge*` emits a union that already excludes the deleted rows.
  - `pruneDeletions(snap, records)` (pure, unit-tested): removes each
    `DocumentKey` from the snapshot, **scoped by `Tablename`** so a sys_id
    collision across tables can't prune the wrong record:
    - `sys_user`/`sys_user_group`/`sys_user_role` → delete from
      `Users`/`Groups`/`Roles` (a deleted group also drops its `GroupMembers`
      bucket).
    - `sys_user_grmember`/`sys_user_has_role`/`sys_group_has_role` → the
      `documentkey` is the JOIN-ROW sys_id, so `pruneNested` removes that row
      from whichever nested `GroupMembers`/`UserRoles`/`GroupRoles` bucket holds
      it (and drops the bucket if it empties). A membership delete does NOT prune
      the user/group/role itself.
  - **Graceful degradation:** any error fetching `sys_audit_delete` (auditing
    disabled, no read access, any 4xx/5xx) is logged at WARN and swallowed —
    `Reconcile` has no error return and NEVER fails the sync and NEVER advances
    the delete watermark. Deletions just aren't captured that run; the periodic
    full-sync backstop reconciles them.
- `connector.go` passes the client as the `Deleter`; `user/group/role.go` call
  `state.Reconcile(ctx)` at the top of each incremental branch (idempotent).
- `stateVersion` bumped 1 → 2: a v1 file unmarshals fine, but the version check
  forces one clean full pull on upgrade so the delete watermark starts from a
  known-good snapshot rather than mid-history.

#### Caveats (deletion capture is best-effort; full sync remains the backstop)

1. **Delete-auditing is per-table / per-instance configurable.** A table whose
   dictionary has auditing off logs no `sys_audit_delete` rows, so its hard
   deletes are invisible to this mechanism on that instance.
2. **`sys_audit_delete` is retention/rotation-limited.** Old audit rows are
   pruned by the instance's audit retention policy; a delete older than the
   stored `delete_watermark` (or rotated out before the next sync) is missed.
3. **Non-audited delete paths exist.** Operations like `setWorkflow(false)`,
   `deleteMultiple()` in some paths, or direct DB ops can remove rows without an
   audit entry.

Because of all three, **a PERIODIC FULL SYNC remains the authoritative backstop**
(run with `--incremental` off, or delete the state file): it rebuilds the
snapshot from the live id-set and reconciles anything the audit stream missed.
Deletion capture makes warm syncs *correct most of the time*; the full sync
guarantees eventual convergence.

### Watermark freeze within a run (`readWatermarks`) — correctness fix

Found while live-testing deletion capture. The per-group / per-role grant
fetches (`group_members`, `user_roles`, `group_roles`) all share ONE stream
watermark, and each `Merge*` advances that stored watermark inline. Because
those fetches happen across many parallel calls **within a single sync**, a row
with a far-future `sys_updated_on` (the `dev289997` demo data has memberships
dated 2031) processed for an early group would push the stored watermark past
2031 mid-sync; every group/role processed afterward then read the advanced
watermark and fetched ZERO of its rows — silently dropping most grants on every
warm sync (observed: 71 grants instead of ~4900).

Fix: `State.Watermark(stream)` now FREEZES the value at the first read per run
(`readWatermarks` map). Every per-group/per-role fetch in one sync uses the same
lower bound; the stored watermark still advances for the *next* run. This is a
pre-existing latent bug in the grant-delta path, exposed (and fixed) here.

### Atomic persist hardening (`os.CreateTemp`)

Also found live: `persist()` used a fixed `"<path>.tmp"` temp name. The baton-sdk
can drive the connector across multiple goroutines/instances sharing one state
path; two writers then collide on the same temp file and one's `rename` hits a
temp the other already renamed away (`ENOENT`), failing the sync. `persist` now
writes to a UNIQUE `os.CreateTemp(dir, base+".tmp-*")` file per call, then
renames — each commit is atomic and independent.

### `pkg/incremental/state.go` (new) + `state_test.go` (new)
The core. `State` is a thread-safe handle to a per-deployment JSON snapshot
(`baton-servicenow-incremental-<deployment>.json` under `--state-dir`).
- `Load(dir, deployment, enabled)` — disabled ⇒ no-op State; missing/corrupt/
  version-mismatched file ⇒ empty snapshot (full pull), never an error.
- `Watermark(stream)` — per-stream `sys_updated_on` lower bound (empty = full).
- `MergeUsers/Roles/Groups/GroupMembers/UserRoles/GroupRoles(...)` — upsert
  changed rows by `sys_id` over the cached snapshot, advance THAT stream's
  watermark to the max `sys_updated_on` seen, **persist inline**, and return the
  full merged union.
- `MarkFailed()` — blocks all further persistence for the run.
- **Per-stream watermarks**: a failure in one stream never advances another's.
- **Why persist inline (not on Close):** the SDK's connector wrapper
  (`internal/connector/connector.go` `wrapper.Close`) does NOT forward `Close`
  to the connector implementation during an in-process c1z sync — verified.
  So a `Close`-time save would never fire. Each `Merge*` therefore writes
  atomically (temp file + rename). `ServiceNow.Close` still calls `Save` as a
  best-effort fallback for plugin/server mode.

### `pkg/connector/connector.go`
- `ServiceNow` gains a `*incremental.State`; `New(...)` takes
  `incrementalEnabled bool, stateDir string` and loads the state.
- `Close(ctx)` best-effort `state.Save()`.

### `pkg/connector/{user,group,role}.go`
- Each `List` (and group `Grants`, role `Grants`) gets an incremental branch
  guarded by `state.Enabled()`: drain the delta via `GetAll…UpdatedSince`, merge,
  emit the full union as a single page (`nextPageToken == ""`). On any error,
  `MarkFailed()` then return. The original paginated full-pull path is unchanged
  for the non-incremental case. Extracted `…ToResources`/`…ToGrants` helpers so
  both paths share resource construction.

### `pkg/config/config.go` + `pkg/config/conf.gen.go` (regenerated)
- `--incremental` (bool, default false) and `--state-dir` (string, CLI-only).
- `cmd/baton-servicenow/main.go` passes `snc.Incremental, snc.StateDir`.

## 5. Build / vet / test

`go build ./... && go vet ./... && go test ./... -count=1` — all pass.
New tests: `pkg/incremental/state_test.go` (disabled no-op, first-sync→delta
merge + per-stream watermark advance, failed-run-does-not-persist, corrupt-cache
→ full pull, per-group member isolation; PLUS deletion: prune-resources,
prune-join-rows, tablename-scoping-prevents-cross-table-prune, Reconcile prunes +
advances delete watermark + runs once-per-run, Reconcile degrades gracefully on
audit error, Reconcile no-op when disabled; PLUS watermark-frozen-within-run);
`pkg/servicenow/request_test.go` (`appendUpdatedSince`). Existing `model_test.go`
still green.

## 6. Live test (deployment `dev289997`)

- **Sync 1** (`--incremental`, empty state): full pull, exit 0.
  State file written (~455 KB): 637 users, 55 groups, 733 roles, all memberships;
  per-stream watermarks populated (e.g. `users: 2026-05-18 19:03:34`). Note the
  dev instance has demo rows with future `sys_updated_on` (e.g. 2031), so some
  watermarks are far-future — harmless, it's just max-observed.
- **Sync 2** (`--incremental`, warm state): each `List`/`Grants` now fetches only
  rows `>=` its watermark (≈0 changed on the idle instance) yet the c1z still
  reports the **full** set carried forward from the snapshot (role grants phase
  shows `total: 733`). Confirms the merge keeps the c1z complete while the API
  pulls only deltas. (See run logs in the test dir.)

### 6b. Live deletion test (deployment `dev289997`)

End-to-end proof that a hard delete is reconciled out of the c1z:

1. Created `INC-DEL-TEST-user`, `INC-DEL-TEST-group`, and a `sys_user_grmember`
   row linking them.
2. **Cold sync** (`--incremental`, empty state): c1z has **4928 grants**, the
   test membership grant is **PRESENT**; the membership row is in the snapshot's
   `group_members[testgroup]` bucket (200 member rows total).
3. Hard-deleted the `sys_user_grmember` row (`DELETE` → 204). Confirmed a
   `sys_audit_delete` row appeared: `tablename=sys_user_grmember`,
   `documentkey=<membership sys_id>`, `sys_created_on=2026-06-19 04:17:27`.
4. **Warm sync** (`--incremental`, warm state): c1z has **4927 grants** (exactly
   one fewer), the test membership grant is **GONE**. Snapshot shows the join row
   pruned (`group_members` 200 → 199) and `delete_watermark` advanced to
   `2026-06-19 04:17:27`. The rest of the access graph carried forward intact
   (638 users / 733 roles / 56 groups). Test records cleaned up afterward.

This also surfaced and fixed the watermark-freeze bug (warm syncs were emitting
71 grants instead of ~4900 before the fix) and the fixed-temp-file persist race.

## 7. Blockers / open questions

- **Deletions** are now captured via `sys_audit_delete` (see "Deletion capture"
  above) and pruned from the snapshot before the union is built. This is
  best-effort (per-table auditing, audit retention, non-audited delete paths —
  the three caveats above), so a **periodic full sync remains the backstop**.
  Soft-deactivation (`active=false`) is still captured by the update delta (it
  bumps `sys_updated_on`).
- **Non-UTC service accounts**: the watermark assumes the API session is UTC
  (verified true for the test account). For a non-UTC account the stored
  watermark would need timezone normalization, or the query should use
  `javascript:gs.dateGenerate(...)` which is unambiguous.
- **State durability/portability**: state lives in a local file keyed by
  deployment. In a hosted/ephemeral runner the `--state-dir` must point at
  persistent storage, else every run is a full pull (safe, just not cheaper).
- **Per-resource grant deltas cost one API call per role/group** even when
  nothing changed (each does a tiny `>=watermark` query). That is far cheaper
  than re-pulling all bindings, but still O(resources) round-trips; a future
  optimization could pull the whole `sys_user_has_role`/`sys_group_has_role`
  delta once and fan it out by role.

## 8. On-call "schedule" resource is on a SEPARATE branch

The on-call **schedule / roster-membership** resource type does **not** exist on
`main` — it lives on `feat/on-call-roster-membership` (the other active branch).
This incremental work covers only the `main` resource types (user, role, group +
their entitlements/grants). At merge time the on-call schedule/roster syncers
will need the same treatment:

1. Add `SysUpdatedOn` to the on-call model structs and the relevant
   `sysparm_fields` lists (`cmn_rota`/`cmn_rota_member` or whichever tables back
   the roster).
2. Add a new `incremental.Stream` (e.g. `StreamOnCallRosters`,
   `StreamRosterMembers`) and `Merge*` methods + snapshot maps in
   `pkg/incremental/state.go`, mirroring the group-members pattern.
3. Add `…UpdatedSince` / `GetAll…UpdatedSince` client methods and the
   `state.Enabled()` incremental branch in the on-call syncer.
4. **Deletion capture for `cmn_rota_member`:** add `cmn_rota_member` (and any
   other on-call join/resource table) to `servicenow.AuditedTables` and add a
   `pruneDeletions` case for it. As with the existing join tables, the
   `sys_audit_delete.documentkey` for a roster-membership delete is the
   roster-membership row's sys_id (the nested-map row key), NOT the user/roster
   sys_id — so it prunes via `pruneNested` over the on-call nested snapshot map.
   Confirm on-call tables are delete-audited on the target instance (they were
   not part of the `dev289997` verification done here).

Roster membership is exactly the kind of frequently-changing data the
incremental feature is meant to keep cheaply current, so it is the highest-value
candidate for this treatment.
