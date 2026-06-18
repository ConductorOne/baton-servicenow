# On-Call Roster Membership Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add ServiceNow on-call **roster membership** as a new synced, provisionable resource type in `baton-servicenow`, so ConductorOne can review and grant/revoke who is on a group's on-call roster.

**Architecture:** On-call data lives in the same ServiceNow Table API the connector already uses. We add a `roster` resource type (`cmn_rota_roster`, GROUP trait) with a single `member` assignment entitlement. Grants are read from `cmn_rota_member` (the user↔roster join), and provisioning POSTs/DELETEs rows in `cmn_rota_member`. The implementation is a near-exact mirror of the existing `group` resource type (`pkg/connector/group.go` + `sys_user_grmember`). Scheduling/rotation/escalation data is intentionally **out of scope** — only roster membership is governance-relevant.

**Tech Stack:** Go, Baton SDK (`connectorbuilder.ResourceSyncer`), ServiceNow Table API.

## Global Constraints

- Go module: `github.com/conductorone/baton-servicenow`. Follow all conventions in `baton-servicenow/CLAUDE.md`.
- Error wrapping: always prefix `baton-servicenow:` and use `%w` (never `%v` for errors).
- Pagination: pass the API's `nextPageToken` through the `pagination.Bag`; never hardcode `""` or a fixed token (see `CLAUDE.md` → "Pagination Termination").
- Reference fields: ALWAYS request explicit `sysparm_fields` (via `FilterVars.Fields`) on every rota query so the Table API returns reference fields as flat sys_id strings, not `{link,value}` objects — this is exactly why `GroupMember.User`/`Group` are plain `string`s today.
- Resource page size: reuse the package var `ResourcesPageSize` (currently 50).
- IDs: `cmn_*` reference fields and `sys_id` are 32-char hex strings; model them as `string`. No numeric-ID risk here, so `json.Number` is not needed.
- Do NOT add scheduling/rotation/escalation/Notify data — roster membership only.
- All new exported client methods take `ctx context.Context` as the first argument and return `(..., error)` with the package error prefix.

---

## Task 0: Verify the on-call table schema against a live instance

**Files:** none (pre-flight verification — no code).

This plan commits to specific `cmn_*` field names. The official docs page would not render the field table during planning, so **confirm these against a real instance before writing code.** A free ServiceNow developer instance (https://developer.servicenow.com/dev.do) with the "On-Call Scheduling" plugin works.

Expected schema (the values the rest of the plan assumes):

| Table | Field | Meaning |
|---|---|---|
| `cmn_rota` | `sys_id` | rotation id |
| `cmn_rota` | `name` | rotation name |
| `cmn_rota` | `group` | reference → `sys_user_group` |
| `cmn_rota_roster` | `sys_id` | roster id |
| `cmn_rota_roster` | `name` | roster name |
| `cmn_rota_roster` | `rota` | reference → `cmn_rota` |
| `cmn_rota_member` | `sys_id` | membership row id |
| `cmn_rota_member` | `roster` | reference → `cmn_rota_roster` |
| `cmn_rota_member` | `member` | reference → `sys_user` (the on-call person) |
| `cmn_rota_member` | `order` | rotation order (string) |

- [ ] **Step 1: Query each table and confirm the field names above**

Run (substitute your instance + creds):

```bash
INSTANCE=devXXXXX
AUTH='user:password'
for t in cmn_rota cmn_rota_roster cmn_rota_member; do
  echo "=== $t ==="
  curl -s -u "$AUTH" \
    "https://$INSTANCE.service-now.com/api/now/table/$t?sysparm_limit=1&sysparm_exclude_reference_link=true" \
    | python3 -m json.tool
done
```

Expected: each response is `{"result":[{...}]}`. Confirm `cmn_rota_member` rows contain keys `roster` and `member` (both 32-char sys_ids), and `cmn_rota_roster` rows contain `rota`.

- [ ] **Step 2: Record any deviations**

If a field name differs (e.g. the user field is not `member`, or roster→rota link differs), update the constants/struct tags in Tasks 1–2 accordingly before proceeding. **Do not guess past a confirmed mismatch.**

- [ ] **Step 3: Confirm reference fields come back as flat strings when `sysparm_fields` is used**

```bash
curl -s -u "$AUTH" \
  "https://$INSTANCE.service-now.com/api/now/table/cmn_rota_member?sysparm_limit=1&sysparm_fields=sys_id,roster,member,order" \
  | python3 -m json.tool
```

Expected: `roster` and `member` are plain sys_id strings (NOT objects). If they come back as `{"link":...,"value":...}` objects even with `sysparm_fields`, add a custom `UnmarshalJSON` to `RotaMember` in Task 1 (pattern: try string, fall back to `struct{Value string}` and take `.Value`) — note this contingency now.

---

## Task 1: Data models + JSON unmarshal test

**Files:**
- Modify: `pkg/servicenow/model.go` (add types after the `GroupMemberPayload` block, ~line 87)
- Test: `pkg/servicenow/model_test.go` (add a new test function)

**Interfaces:**
- Produces: types `Roster`, `Rota`, `RotaMember`, `RotaMemberPayload` (package `servicenow`). `RotaMember{ BaseResource; Roster string; Member string; Order string }`, `RotaMemberPayload{ Roster string; Member string }`, `Roster{ BaseResource; Name string; Rota string }`, `Rota{ BaseResource; Name string; Group string }`.

- [ ] **Step 1: Write the failing test**

Add to `pkg/servicenow/model_test.go`:

```go
func TestRotaMember_Unmarshal(t *testing.T) {
	const input = `{"sys_id":"mem123","roster":"ros456","member":"usr789","order":"1"}`

	var got RotaMember
	if err := json.Unmarshal([]byte(input), &got); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got.Id != "mem123" {
		t.Errorf("Id = %q, want mem123", got.Id)
	}
	if got.Roster != "ros456" {
		t.Errorf("Roster = %q, want ros456", got.Roster)
	}
	if got.Member != "usr789" {
		t.Errorf("Member = %q, want usr789", got.Member)
	}
	if got.Order != "1" {
		t.Errorf("Order = %q, want 1", got.Order)
	}
}

func TestRoster_Unmarshal(t *testing.T) {
	const input = `{"sys_id":"ros456","name":"Primary","rota":"rot111"}`

	var got Roster
	if err := json.Unmarshal([]byte(input), &got); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Id != "ros456" || got.Name != "Primary" || got.Rota != "rot111" {
		t.Errorf("got %+v, want {Id:ros456 Name:Primary Rota:rot111}", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/servicenow/ -run 'TestRotaMember_Unmarshal|TestRoster_Unmarshal' -v`
Expected: FAIL — `undefined: RotaMember` / `undefined: Roster`.

- [ ] **Step 3: Add the model types**

In `pkg/servicenow/model.go`, immediately after the `GroupMemberPayload` struct (line ~87):

```go
// Table `cmn_rota` (On-Call Rotation). One rotation belongs to a group.
type Rota struct {
	BaseResource
	Name  string `json:"name"`
	Group string `json:"group"`
}

// Table `cmn_rota_roster` (On-Call Roster). A roster belongs to a rota.
type Roster struct {
	BaseResource
	Name string `json:"name"`
	Rota string `json:"rota"`
}

// Table `cmn_rota_member` (On-Call Roster Member). Joins a user to a roster.
type RotaMember struct {
	BaseResource
	Roster string `json:"roster"`
	Member string `json:"member"`
	Order  string `json:"order"`
}

type RotaMemberPayload struct {
	Roster string `json:"roster"`
	Member string `json:"member"`
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `go test ./pkg/servicenow/ -run 'TestRotaMember_Unmarshal|TestRoster_Unmarshal' -v`
Expected: PASS (both tests).

- [ ] **Step 5: Commit**

```bash
git add pkg/servicenow/model.go pkg/servicenow/model_test.go
git commit -m "feat: add on-call rota/roster/member data models"
```

---

## Task 2: URL constants, response aliases, and filter builders + test

**Files:**
- Modify: `pkg/servicenow/client.go` (URL constants ~line 41, response-type aliases ~line 96)
- Modify: `pkg/servicenow/request.go` (field lists ~line 13, filter builders ~line 157)
- Test: `pkg/servicenow/model_test.go` (add filter-builder test)

**Interfaces:**
- Consumes: `FilterVars` (from `request.go`), `ListResponse[T]` (from `client.go`).
- Produces:
  - Constants `RostersBaseUrl`, `RotaMembersBaseUrl`, `RotaMemberDetailBaseUrl` (package `servicenow`).
  - Aliases `RostersResponse = ListResponse[Roster]`, `RotaMembersResponse = ListResponse[RotaMember]`.
  - `func prepareRosterFilters() *FilterVars`
  - `func prepareRotaMemberFilter(rosterId string, memberId string) *FilterVars` — builds query `roster=<id>^member=<id>` (omitting empty parts), Fields `["sys_id","roster","member","order"]`.

- [ ] **Step 1: Write the failing test**

Add to `pkg/servicenow/model_test.go`:

```go
func TestPrepareRotaMemberFilter(t *testing.T) {
	tests := []struct {
		name      string
		rosterId  string
		memberId  string
		wantQuery string
	}{
		{"roster only", "ros456", "", "roster=ros456"},
		{"both", "ros456", "usr789", "roster=ros456^member=usr789"},
		{"member only", "", "usr789", "member=usr789"},
		{"neither", "", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := prepareRotaMemberFilter(tt.rosterId, tt.memberId)
			if got.Query != tt.wantQuery {
				t.Errorf("Query = %q, want %q", got.Query, tt.wantQuery)
			}
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./pkg/servicenow/ -run TestPrepareRotaMemberFilter -v`
Expected: FAIL — `undefined: prepareRotaMemberFilter`.

- [ ] **Step 3: Add URL constants**

In `pkg/servicenow/client.go`, in the `const (...)` block after the group-member URLs (~line 35):

```go
	// On-Call Scheduling.
	RostersBaseUrl          = TableAPIBaseURL + "/cmn_rota_roster"
	RotaMembersBaseUrl      = TableAPIBaseURL + "/cmn_rota_member"
	RotaMemberDetailBaseUrl = RotaMembersBaseUrl + "/%s"
```

- [ ] **Step 4: Add response-type aliases**

In `pkg/servicenow/client.go`, after `GroupMembersResponse` (~line 93):

```go
type RostersResponse = ListResponse[Roster]
type RotaMembersResponse = ListResponse[RotaMember]
```

- [ ] **Step 5: Add filter builders**

In `pkg/servicenow/request.go`, after `prepareUserToGroupFilter` (~line 157):

```go
func prepareRosterFilters() *FilterVars {
	return &FilterVars{
		Fields: []string{"sys_id", "name", "rota"},
	}
}

func prepareRotaMemberFilter(rosterId string, memberId string) *FilterVars {
	var query string

	if rosterId != "" {
		query = fmt.Sprintf("roster=%s", rosterId)
	}

	if memberId != "" {
		if query != "" {
			query = fmt.Sprintf("%s^member=%s", query, memberId)
		} else {
			query = fmt.Sprintf("member=%s", memberId)
		}
	}

	return &FilterVars{
		Fields: []string{"sys_id", "roster", "member", "order"},
		Query:  query,
	}
}
```

- [ ] **Step 6: Run test + build to verify pass**

Run: `go test ./pkg/servicenow/ -run TestPrepareRotaMemberFilter -v && go build ./...`
Expected: PASS, build succeeds.

- [ ] **Step 7: Commit**

```bash
git add pkg/servicenow/client.go pkg/servicenow/request.go pkg/servicenow/model_test.go
git commit -m "feat: add on-call roster/member URLs, responses, and filters"
```

---

## Task 3: API client methods

**Files:**
- Modify: `pkg/servicenow/client.go` (add methods after `RemoveUserFromGroup`, ~line 290)

**Interfaces:**
- Consumes: `c.get/post/delete`, `c.apiURL`, `filterToReqOptions`, `paginationVarsToReqOptions`, `PaginationVars`, the constants/filters/aliases from Task 2.
- Produces (all methods on `*Client`):
  - `GetRosters(ctx, paginationVars PaginationVars) ([]Roster, string, error)`
  - `GetRotaMembers(ctx, rosterId string, memberId string, paginationVars PaginationVars) ([]RotaMember, string, error)`
  - `AddUserToRoster(ctx, record RotaMemberPayload) error`
  - `RemoveRotaMember(ctx, id string) error`

These mirror `GetGroups` / `GetUserToGroup` / `AddUserToGroup` / `RemoveUserFromGroup` exactly.

- [ ] **Step 1: Add the client methods**

In `pkg/servicenow/client.go`, after `RemoveUserFromGroup` (~line 290):

```go
// Table `cmn_rota_roster` (On-Call Rosters).
func (c *Client) GetRosters(ctx context.Context, paginationVars PaginationVars) ([]Roster, string, error) {
	var rostersResponse RostersResponse

	reqOpts := filterToReqOptions(prepareRosterFilters())
	reqOpts = append(reqOpts, paginationVarsToReqOptions(&paginationVars)...)

	nextPageToken, err := c.get(
		ctx,
		c.apiURL(RostersBaseUrl, c.deployment),
		&rostersResponse,
		reqOpts...,
	)
	if err != nil {
		return nil, "", err
	}

	return rostersResponse.Result, nextPageToken, nil
}

// Table `cmn_rota_member` (On-Call Roster Members).
func (c *Client) GetRotaMembers(ctx context.Context, rosterId string, memberId string, paginationVars PaginationVars) ([]RotaMember, string, error) {
	var rotaMembersResponse RotaMembersResponse

	reqOpts := filterToReqOptions(prepareRotaMemberFilter(rosterId, memberId))
	reqOpts = append(reqOpts, paginationVarsToReqOptions(&paginationVars)...)

	nextPageToken, err := c.get(
		ctx,
		c.apiURL(RotaMembersBaseUrl, c.deployment),
		&rotaMembersResponse,
		reqOpts...,
	)
	if err != nil {
		return nil, "", err
	}

	return rotaMembersResponse.Result, nextPageToken, nil
}

func (c *Client) AddUserToRoster(ctx context.Context, record RotaMemberPayload) error {
	return c.post(
		ctx,
		c.apiURL(RotaMembersBaseUrl, c.deployment),
		nil,
		&record,
		WithIncludeResponseBody(),
	)
}

func (c *Client) RemoveRotaMember(ctx context.Context, id string) error {
	return c.delete(
		ctx,
		c.apiURL(RotaMemberDetailBaseUrl, c.deployment, id),
		nil,
	)
}
```

- [ ] **Step 2: Build to verify it compiles**

Run: `go build ./... && go vet ./pkg/servicenow/`
Expected: no output (success).

- [ ] **Step 3: Commit**

```bash
git add pkg/servicenow/client.go
git commit -m "feat: add on-call roster/member client methods"
```

---

## Task 4: Roster resource type — List, Entitlements, Grants

**Files:**
- Create: `pkg/connector/roster.go`
- Modify: `pkg/connector/helpers.go` (add `mapRotaMembers`, ~line 54)

**Interfaces:**
- Consumes: `resourceTypeRoster` (defined in Task 5 — declare it there; this task references it), `resourceTypeUser`, `ResourcesPageSize`, `parsePageToken`, client methods from Task 3, `mapRotaMembers` (added here).
- Produces: `func rosterBuilder(client *servicenow.Client) *rosterResourceType`, constant `rosterMembership = "member"`, `func mapRotaMembers([]servicenow.RotaMember) []string`.

> **Note on task order:** `resourceTypeRoster` is declared in Task 5 (connector.go). Because Go compiles the whole package, `roster.go` will not build until Task 5 adds that var. Implement Task 4 and Task 5 back-to-back; the build/verify gate is at the end of Task 5. (If executing strictly task-by-task with a build gate per task, temporarily add a local `var resourceTypeRoster = &v2.ResourceType{Id:"roster", DisplayName:"On-Call Roster", Traits:[]v2.ResourceType_Trait{v2.ResourceType_TRAIT_GROUP}}` at the top of roster.go, then MOVE it to connector.go in Task 5 — do not leave a duplicate.)

- [ ] **Step 1: Add `mapRotaMembers` helper**

In `pkg/connector/helpers.go`, after `mapGroupMembers` (~line 54):

```go
func mapRotaMembers(resources []servicenow.RotaMember) []string {
	members := make([]string, len(resources))

	for i, r := range resources {
		members[i] = r.Member
	}

	return members
}
```

- [ ] **Step 2: Create `roster.go` with List/Entitlements/Grants**

Create `pkg/connector/roster.go`:

```go
package connector

import (
	"context"
	"fmt"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-servicenow/pkg/servicenow"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type rosterResourceType struct {
	resourceType *v2.ResourceType
	client       *servicenow.Client
}

func (r *rosterResourceType) ResourceType(_ context.Context) *v2.ResourceType {
	return r.resourceType
}

const rosterMembership = "member"

// Create a new connector resource for a ServiceNow on-call roster.
func rosterResource(roster *servicenow.Roster) (*v2.Resource, error) {
	profile := map[string]interface{}{
		"roster_name": roster.Name,
		"roster_id":   roster.Id,
		"rota_id":     roster.Rota,
	}

	groupTraitOptions := []rs.GroupTraitOption{
		rs.WithGroupProfile(profile),
	}

	displayName := roster.Name
	if displayName == "" {
		displayName = roster.Id
	}

	resource, err := rs.NewGroupResource(
		displayName,
		resourceTypeRoster,
		roster.Id,
		groupTraitOptions,
	)
	if err != nil {
		return nil, err
	}

	return resource, nil
}

func (r *rosterResourceType) List(ctx context.Context, _ *v2.ResourceId, pt *pagination.Token) ([]*v2.Resource, string, annotations.Annotations, error) {
	bag, offset, err := parsePageToken(pt.Token, &v2.ResourceId{ResourceType: resourceTypeRoster.Id})
	if err != nil {
		return nil, "", nil, err
	}

	rosters, nextPageToken, err := r.client.GetRosters(
		ctx,
		servicenow.PaginationVars{
			Limit:  ResourcesPageSize,
			Offset: offset,
		},
	)
	if err != nil {
		return nil, "", nil, fmt.Errorf("baton-servicenow: failed to list rosters: %w", err)
	}

	nextPage, err := bag.NextToken(nextPageToken)
	if err != nil {
		return nil, "", nil, err
	}

	var rv []*v2.Resource
	for _, roster := range rosters {
		rosterCopy := roster
		rr, err := rosterResource(&rosterCopy)
		if err != nil {
			return nil, "", nil, err
		}
		rv = append(rv, rr)
	}

	return rv, nextPage, nil, nil
}

func (r *rosterResourceType) Entitlements(ctx context.Context, resource *v2.Resource, _ *pagination.Token) ([]*v2.Entitlement, string, annotations.Annotations, error) {
	var rv []*v2.Entitlement

	assignmentOptions := []ent.EntitlementOption{
		ent.WithGrantableTo(resourceTypeUser),
		ent.WithDisplayName(fmt.Sprintf("%s Roster %s", resource.DisplayName, rosterMembership)),
		ent.WithDescription(fmt.Sprintf("On-call membership in %s roster in ServiceNow", resource.DisplayName)),
	}

	rv = append(rv, ent.NewAssignmentEntitlement(
		resource,
		rosterMembership,
		assignmentOptions...,
	))

	return rv, "", nil, nil
}

func (r *rosterResourceType) Grants(ctx context.Context, resource *v2.Resource, pt *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
	bag, offset, err := parsePageToken(pt.Token, &v2.ResourceId{ResourceType: resourceTypeRoster.Id})
	if err != nil {
		return nil, "", nil, err
	}

	rotaMembers, nextPageToken, err := r.client.GetRotaMembers(
		ctx,
		resource.Id.Resource, // all members of this roster
		"",
		servicenow.PaginationVars{
			Limit:  ResourcesPageSize,
			Offset: offset,
		},
	)
	if err != nil {
		return nil, "", nil, fmt.Errorf("baton-servicenow: failed to list roster members: %w", err)
	}

	nextPage, err := bag.NextToken(nextPageToken)
	if err != nil {
		return nil, "", nil, err
	}

	memberIDs := mapRotaMembers(rotaMembers)
	if len(memberIDs) == 0 {
		return []*v2.Grant{}, nextPage, nil, nil
	}

	var rv []*v2.Grant
	for _, member := range memberIDs {
		rID, err := rs.NewResourceID(resourceTypeUser, member)
		if err != nil {
			return nil, "", nil, fmt.Errorf("baton-servicenow: error creating principal id")
		}

		rv = append(rv, grant.NewGrant(resource, rosterMembership, rID))
	}

	return rv, nextPage, nil, nil
}

func rosterBuilder(client *servicenow.Client) *rosterResourceType {
	return &rosterResourceType{
		resourceType: resourceTypeRoster,
		client:       client,
	}
}

// Grant/Revoke implemented in Task 5.
var (
	_ = ctxzap.Extract
	_ = zap.String
)
```

> The trailing `var (_ = ...)` block prevents "imported and not used" errors for `ctxzap`/`zap` between Task 4 and Task 5. **Delete this block in Task 5** once `Grant`/`Revoke` use those imports.

- [ ] **Step 3: Defer build verification to Task 5**

Do not build yet — `resourceTypeRoster` is added in Task 5. Proceed directly to Task 5. (No commit yet; Task 4 + Task 5 land together.)

---

## Task 5: Provisioning (Grant/Revoke) + register the resource type

**Files:**
- Modify: `pkg/connector/roster.go` (add `Grant`/`Revoke`, remove the temporary `var (_ = ...)` block)
- Modify: `pkg/connector/connector.go` (add `resourceTypeRoster` var ~line 38; add `rosterBuilder(s.client)` to `ResourceSyncers` ~line 50)

**Interfaces:**
- Consumes: `rosterResourceType` + `rosterBuilder` (Task 4), client methods `GetRotaMembers`/`AddUserToRoster`/`RemoveRotaMember` (Task 3), `RotaMemberPayload` (Task 1).
- Produces: `resourceTypeRoster *v2.ResourceType` (Id `"roster"`, GROUP trait); `rosterResourceType.Grant` / `.Revoke` (satisfy the provisioning interface, mirroring `group.go`).

- [ ] **Step 1: Declare the resource type**

In `pkg/connector/connector.go`, inside the `var (...)` block after `resourceTypeGroup` (~line 38):

```go
	resourceTypeRoster = &v2.ResourceType{
		Id:          "roster",
		DisplayName: "On-Call Roster",
		Traits: []v2.ResourceType_Trait{
			v2.ResourceType_TRAIT_GROUP,
		},
	}
```

- [ ] **Step 2: Register the syncer**

In `pkg/connector/connector.go`, in `ResourceSyncers` (~line 46), add `rosterBuilder`:

```go
func (s *ServiceNow) ResourceSyncers(ctx context.Context) []connectorbuilder.ResourceSyncer {
	return []connectorbuilder.ResourceSyncer{
		userBuilder(s.client),
		roleBuilder(s.client),
		groupBuilder(s.client),
		rosterBuilder(s.client),
	}
}
```

- [ ] **Step 3: Add Grant/Revoke and remove the temporary var block**

In `pkg/connector/roster.go`, delete the `var (_ = ctxzap.Extract; _ = zap.String)` block and replace it with:

```go
func (r *rosterResourceType) Grant(ctx context.Context, principal *v2.Resource, entitlement *v2.Entitlement) (annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	if principal.Id.ResourceType != resourceTypeUser.Id {
		l.Warn(
			"baton-servicenow: only users can be added to an on-call roster",
			zap.String("principal_type", principal.Id.ResourceType),
			zap.String("principal_id", principal.Id.Resource),
		)
		return nil, nil
	}

	rosterId := entitlement.Resource.Id.Resource
	existing, _, err := r.client.GetRotaMembers(
		ctx,
		rosterId,
		principal.Id.Resource,
		servicenow.PaginationVars{Limit: 1},
	)
	if err != nil {
		return nil, fmt.Errorf("baton-servicenow: failed to get roster members for %s: %w", entitlement.Id, err)
	}

	// check if user is already on the roster
	if len(existing) > 0 {
		l.Warn(
			"baton-servicenow: cannot add user who is already on the roster",
			zap.String("roster", entitlement.Id),
			zap.String("user", principal.Id.Resource),
		)
		return annotations.New(&v2.GrantAlreadyExists{}), nil
	}

	err = r.client.AddUserToRoster(
		ctx,
		servicenow.RotaMemberPayload{
			Roster: rosterId,
			Member: principal.Id.Resource,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("baton-servicenow: failed to add user %s to roster %s: %w", principal.Id.Resource, rosterId, err)
	}

	return nil, nil
}

func (r *rosterResourceType) Revoke(ctx context.Context, grant *v2.Grant) (annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	entitlement := grant.Entitlement
	principal := grant.Principal

	if principal.Id.ResourceType != resourceTypeUser.Id {
		l.Warn(
			"baton-servicenow: only users can be removed from an on-call roster",
			zap.String("principal_type", principal.Id.ResourceType),
			zap.String("principal_id", principal.Id.Resource),
		)
	}

	rosterId := entitlement.Resource.Id.Resource
	rotaMembers, _, err := r.client.GetRotaMembers(
		ctx,
		rosterId,
		principal.Id.Resource,
		servicenow.PaginationVars{Limit: 1},
	)
	if err != nil {
		return nil, fmt.Errorf("baton-servicenow: failed to get roster members for %s: %w", grant.Principal.Id.Resource, err)
	}

	// check if user is on the roster at all
	if len(rotaMembers) == 0 {
		l.Warn(
			"baton-servicenow: cannot remove user from a roster they are not on",
			zap.String("roster", entitlement.Id),
			zap.String("user", principal.Id.Resource),
		)
		return annotations.New(&v2.GrantAlreadyRevoked{}), nil
	}

	for _, member := range rotaMembers {
		err = r.client.RemoveRotaMember(ctx, member.Id)
		if err != nil {
			return nil, fmt.Errorf("baton-servicenow: failed to remove user %s from roster: %w", grant.Principal.Id.Resource, err)
		}
		l.Debug("revoked roster membership from user", zap.String("roster", grant.Entitlement.Id))
	}

	return nil, nil
}
```

- [ ] **Step 4: Build, vet, and run full test suite**

Run: `go build ./... && go vet ./... && go test ./... -count=1`
Expected: build succeeds, vet clean, all tests PASS (including Task 1/2 tests).

- [ ] **Step 5: Commit**

```bash
git add pkg/connector/roster.go pkg/connector/connector.go pkg/connector/helpers.go
git commit -m "feat: sync and provision on-call roster membership"
```

---

## Task 6: Live sync smoke test against a dev instance

**Files:** none (manual verification — connector syncers have no unit-test harness in this repo, so a real sync is the correctness gate).

- [ ] **Step 1: Build the connector binary**

Run: `go build -o /tmp/baton-servicenow ./cmd/baton-servicenow`
Expected: binary produced.

- [ ] **Step 2: Run a full sync against the dev instance**

```bash
BATON_USERNAME=user BATON_PASSWORD=password BATON_DEPLOYMENT=devXXXXX \
  /tmp/baton-servicenow --file=/tmp/sync.c1z
```

Expected: exits 0, no errors logged. (Use `LOG_LEVEL=debug` if you need to inspect requests.)

- [ ] **Step 3: Verify rosters and grants are present**

```bash
go install github.com/conductorone/baton/cmd/baton@main
baton resources --file=/tmp/sync.c1z | grep -i roster
baton grants    --file=/tmp/sync.c1z | grep -i roster
```

Expected: `On-Call Roster` resources listed; roster `member` grants present for users who are on rosters in the instance. Cross-check one roster's members against `cmn_rota_member` in the ServiceNow UI (Roster → Members).

- [ ] **Step 4: (If a test user/roster is available) verify provisioning round-trips**

```bash
# Grant
baton --file=/tmp/sync.c1z grant  "<roster-entitlement-id>" "<user-resource-id>"
# Confirm the row appears in cmn_rota_member, then revoke
baton --file=/tmp/sync.c1z revoke "<grant-id>"
```

Expected: a `cmn_rota_member` row is created then deleted; re-running grant when already a member returns `GrantAlreadyExists` (no error); revoke when not present returns `GrantAlreadyRevoked`.

- [ ] **Step 5: Record results in the PR description** (no commit).

---

## Task 7: Documentation

**Files:**
- Modify: `README.md` (the "## Data Model" / "Relevant Tables" sections)
- Modify: `CHANGELOG.md` if present (else skip)

**Interfaces:** none.

- [ ] **Step 1: Update README relevant-tables list**

In `README.md`, under "### Relevant Tables:", add:

```markdown
- `cmn_rota_roster` - On-call rosters
- `cmn_rota_member` - On-call roster membership (read + provision)
```

And in the intro line that says it "works with the ServiceNow Table API to sync data about users, groups and roles", change to "...users, groups, roles, and on-call rosters."

- [ ] **Step 2: Note the new ACL requirement**

Add a sentence near the existing ACL paragraph: the connecting account needs read access to `cmn_rota_roster` and `cmn_rota_member`, plus write access to `cmn_rota_member` for provisioning. The `admin` role already covers this.

- [ ] **Step 3: Commit**

```bash
git add README.md CHANGELOG.md 2>/dev/null; git add README.md
git commit -m "docs: document on-call roster membership support"
```

---

## Self-Review

**Spec coverage:**
- Sync who is on each roster → Tasks 3 (`GetRotaMembers`) + 4 (`Grants`). ✅
- Roster as a reviewable resource → Tasks 4/5 (`roster` resource type, GROUP trait, `member` entitlement). ✅
- Provision (add/remove roster membership) → Task 5 (`Grant`/`Revoke` via `cmn_rota_member`). ✅
- Scheduling/rotation/escalation excluded → no tasks touch `cmn_schedule`/rotation/Notify (by design). ✅
- Field-name correctness → Task 0 verification gate. ✅
- Reference-fields-as-strings risk → Task 0 Step 3 + Global Constraints (always send `sysparm_fields`). ✅

**Type consistency:** `RotaMember{Id,Roster,Member,Order}` and `RotaMemberPayload{Roster,Member}` are used identically in Tasks 1/3/5. `GetRotaMembers(ctx, rosterId, memberId, pag)` arg order is consistent across Grants (rosterId set, memberId ""), Grant (both set), Revoke (both set). `resourceTypeRoster.Id == "roster"` used in `parsePageToken` calls and resource construction. `rosterMembership = "member"` used in Entitlements + Grants + grant.NewGrant. ✅

**Placeholder scan:** No TBD/TODO; every code step contains complete code; the only deliberate temporary scaffold (the `var (_ = ...)` block) is explicitly created and removed within the plan. ✅

**Known risk / contingency:** If Task 0 reveals reference fields return as `{link,value}` objects despite `sysparm_fields`, add a `RotaMember.UnmarshalJSON` (string-or-`{Value}` fallback, modeled on `User.UnmarshalJSON`) in Task 1 — flagged in Task 0 Step 3. Field names other than `member`/`roster`/`rota`/`group` must be corrected in Tasks 1–2 before coding.
