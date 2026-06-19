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
→ full pull, per-group member isolation); `pkg/servicenow/request_test.go`
(`appendUpdatedSince`). Existing `model_test.go` still green.

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

## 7. Blockers / open questions

- **Deletions** are not captured by a `sys_updated_on` delta (a hard-deleted row
  has no new timestamp), so deleted users/groups/roles/memberships linger in the
  snapshot until a full sync. Mitigation: run a periodic full sync
  (`--incremental` off, or delete the state file) to reconcile. Soft-deactivation
  (`active=false`) IS captured (it bumps `sys_updated_on`). A future improvement
  could reconcile deletions by periodically diffing the full id-set per stream.
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

Roster membership is exactly the kind of frequently-changing data the
incremental feature is meant to keep cheaply current, so it is the highest-value
candidate for this treatment.
