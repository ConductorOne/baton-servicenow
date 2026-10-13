package connector

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorstore"
	"github.com/conductorone/baton-sdk/pkg/dotc1z"
	"github.com/conductorone/baton-sdk/pkg/dotc1z/c1zstore"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	ent "github.com/conductorone/baton-sdk/pkg/types/entitlement"
	"github.com/conductorone/baton-sdk/pkg/types/grant"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/conductorone/baton-servicenow/pkg/servicenow"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// membershipRow is one row of a membership table; ref is its user or group reference.
type membershipRow struct {
	id  string
	ref string
}

var seekPattern = regexp.MustCompile(`sys_id>([^^]+)`)

// newMembershipServer serves keyset pages of at most pageSize rows from the
// table named by the request path's last segment.
func newMembershipServer(t *testing.T, pageSize int, tables map[string][]membershipRow) *servicenow.Client {
	t.Helper()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		table := r.URL.Path[strings.LastIndex(r.URL.Path, "/")+1:]
		rows := append([]membershipRow(nil), tables[table]...)
		sort.Slice(rows, func(i, j int) bool { return rows[i].id < rows[j].id })

		lastID := ""
		if m := seekPattern.FindStringSubmatch(r.URL.Query().Get("sysparm_query")); m != nil {
			lastID = m[1]
		}
		limit := pageSize
		if l, err := strconv.Atoi(r.URL.Query().Get("sysparm_limit")); err == nil && l < limit {
			limit = l
		}

		var page []map[string]string
		for _, row := range rows {
			if row.id <= lastID || len(page) == limit {
				continue
			}
			page = append(page, map[string]string{"sys_id": row.id, "user": row.ref, "group": row.ref})
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{"result": page}); err != nil {
			t.Errorf("failed to encode test response: %v", err)
		}
	}))
	t.Cleanup(server.Close)

	client, err := servicenow.NewClient(uhttp.NewBaseHttpClient(server.Client()), "Basic dGVzdDp0ZXN0", "dev0", nil, nil, nil, server.URL)
	require.NoError(t, err)
	return client
}

type grantsFunc func(context.Context, *v2.Resource, *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error)

func roleGrants(client *servicenow.Client) grantsFunc {
	return func(ctx context.Context, res *v2.Resource, pt *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
		return roleBuilder(client).Grants(ctx, res, pt)
	}
}

func groupGrants(client *servicenow.Client) grantsFunc {
	return func(ctx context.Context, res *v2.Resource, pt *pagination.Token) ([]*v2.Grant, string, annotations.Annotations, error) {
		return groupBuilder(client).Grants(ctx, res, pt)
	}
}

// pageResult is what one Grants call returned.
type pageResult struct {
	grants    []*v2.Grant
	nextToken string
}

// drainGrants calls fn until pagination ends, the way the SDK syncer does.
func drainGrants(ctx context.Context, t *testing.T, fn grantsFunc, res *v2.Resource) ([]*v2.Grant, []pageResult) {
	t.Helper()

	var all []*v2.Grant
	var pages []pageResult
	token := ""
	for i := 0; ; i++ {
		require.Less(t, i, 100, "pagination did not terminate")
		grants, next, _, err := fn(ctx, res, &pagination.Token{Token: token})
		require.NoError(t, err)
		requireValidPrincipals(t, grants)
		all = append(all, grants...)
		pages = append(pages, pageResult{grants: grants, nextToken: next})
		if next == "" {
			return all, pages
		}
		token = next
	}
}

func requireValidPrincipals(t *testing.T, grants []*v2.Grant) {
	t.Helper()
	for _, g := range grants {
		require.NotEmpty(t, g.GetPrincipal().GetId().GetResource(), "grant %s has an empty principal ID", g.GetId())
		require.NotEmpty(t, g.GetPrincipal().GetId().GetResourceType(), "grant %s has an empty principal type", g.GetId())
		require.NotContains(t, g.GetId(), "::", "grant %s has an empty ID segment", g.GetId())
	}
}

func grantIDs(grants []*v2.Grant) []string {
	ids := make([]string, 0, len(grants))
	for _, g := range grants {
		ids = append(ids, g.GetId())
	}
	return ids
}

func testRole(t *testing.T) *v2.Resource {
	t.Helper()
	res, err := roleResource(&servicenow.Role{BaseResource: servicenow.BaseResource{Id: "R"}, Name: "admin"})
	require.NoError(t, err)
	return res
}

func testGroup(t *testing.T) *v2.Resource {
	t.Helper()
	res, err := groupResource(&servicenow.Group{BaseResource: servicenow.BaseResource{Id: "G"}, Name: "ops"})
	require.NoError(t, err)
	return res
}

func TestRoleGrants_SkipsEmptyUserReferences(t *testing.T) {
	ctx := context.Background()
	mixed := []membershipRow{{"row01", "u1"}, {"row02", ""}, {"row03", "  "}, {"row04", "u2"}}
	baseline := []membershipRow{{"row01", "u1"}, {"row02", "x2"}, {"row03", "x3"}, {"row04", "u2"}}

	grants, pages := drainGrants(ctx, t, roleGrants(newMembershipServer(t, 200, map[string][]membershipRow{"sys_user_has_role": mixed})), testRole(t))
	_, basePages := drainGrants(ctx, t, roleGrants(newMembershipServer(t, 200, map[string][]membershipRow{"sys_user_has_role": baseline})), testRole(t))

	require.Equal(t, []string{"role:R:member:user:u1", "role:R:member:user:u2"}, grantIDs(grants))
	require.Len(t, pages, len(basePages))
	for i := range pages {
		require.Equal(t, basePages[i].nextToken, pages[i].nextToken, "page %d token differs from the unfiltered baseline", i)
	}
}

func TestRoleGrants_AllEmptyUserPageKeepsPaginating(t *testing.T) {
	ctx := context.Background()
	rows := []membershipRow{{"row01", ""}, {"row02", ""}, {"row03", ""}, {"row04", "u1"}}
	client := newMembershipServer(t, 3, map[string][]membershipRow{"sys_user_has_role": rows})

	grants, pages := drainGrants(ctx, t, roleGrants(client), testRole(t))

	// pages[0] only seeds the bag; pages[1] is the all-empty first API page.
	require.Empty(t, pages[1].grants)
	require.NotEmpty(t, pages[1].nextToken)
	require.Equal(t, []string{"role:R:member:user:u1"}, grantIDs(grants))
}

func TestRoleGrants_DuplicateEmptyUserRowsEmitNoGrant(t *testing.T) {
	ctx := context.Background()
	client := newMembershipServer(t, 200, map[string][]membershipRow{"sys_user_has_role": {{"row01", ""}, {"row02", ""}}})

	grants, _ := drainGrants(ctx, t, roleGrants(client), testRole(t))

	require.Empty(t, grants)
}

func TestRoleGrants_SkipsEmptyGroupReferences(t *testing.T) {
	ctx := context.Background()
	client := newMembershipServer(t, 200, map[string][]membershipRow{"sys_group_has_role": {{"row01", "g1"}, {"row02", ""}}})

	grants, _ := drainGrants(ctx, t, roleGrants(client), testRole(t))

	require.Equal(t, []string{"role:R:member:group:g1"}, grantIDs(grants))
	expandable := &v2.GrantExpandable{}
	found := false
	for _, a := range grants[0].GetAnnotations() {
		if a.MessageIs(expandable) {
			require.NoError(t, a.UnmarshalTo(expandable))
			found = true
		}
	}
	require.True(t, found, "group grant lost its GrantExpandable annotation")
	require.Equal(t, []string{"group:g1:member"}, expandable.GetEntitlementIds())
}

func TestRoleGrants_ValidRowsKeepGrantIDs(t *testing.T) {
	ctx := context.Background()
	client := newMembershipServer(t, 200, map[string][]membershipRow{
		"sys_user_has_role":  {{"row01", "u1"}, {"row02", "u2"}},
		"sys_group_has_role": {{"row03", "g1"}},
	})

	grants, _ := drainGrants(ctx, t, roleGrants(client), testRole(t))

	require.ElementsMatch(t, []string{"role:R:member:user:u1", "role:R:member:user:u2", "role:R:member:group:g1"}, grantIDs(grants))
}

func TestGroupGrants_SkipsEmptyUserReferences(t *testing.T) {
	ctx := context.Background()
	client := newMembershipServer(t, 200, map[string][]membershipRow{"sys_user_grmember": {{"row01", ""}, {"row02", "u1"}}})

	grants, _ := drainGrants(ctx, t, groupGrants(client), testGroup(t))

	require.Equal(t, []string{"group:G:member:user:u1"}, grantIDs(grants))
}

func TestGroupGrants_AllEmptyPageKeepsPaginating(t *testing.T) {
	ctx := context.Background()
	rows := []membershipRow{{"row01", ""}, {"row02", " "}, {"row03", "u1"}}
	client := newMembershipServer(t, 2, map[string][]membershipRow{"sys_user_grmember": rows})

	grants, pages := drainGrants(ctx, t, groupGrants(client), testGroup(t))

	require.Empty(t, pages[0].grants)
	require.NotEmpty(t, pages[0].nextToken)
	require.Equal(t, []string{"group:G:member:user:u1"}, grantIDs(grants))
}

// Page size 1 puts the empty row both at the end of a page and alone on a page.
func TestGrants_PageSizeOneEmitsAllValidGrants(t *testing.T) {
	ctx := context.Background()
	client := newMembershipServer(t, 1, map[string][]membershipRow{
		"sys_user_has_role":  {{"row01", "u1"}, {"row02", ""}, {"row03", "u2"}, {"row04", ""}},
		"sys_group_has_role": {{"row05", ""}, {"row06", "g1"}},
		"sys_user_grmember":  {{"row07", ""}, {"row08", "u1"}, {"row09", ""}},
	})

	roleGrantsOut, _ := drainGrants(ctx, t, roleGrants(client), testRole(t))
	groupGrantsOut, _ := drainGrants(ctx, t, groupGrants(client), testGroup(t))

	require.ElementsMatch(t, []string{"role:R:member:user:u1", "role:R:member:user:u2", "role:R:member:group:g1"}, grantIDs(roleGrantsOut))
	require.Equal(t, []string{"group:G:member:user:u1"}, grantIDs(groupGrantsOut))
}

func TestGrants_LogsSkipsAtDebug(t *testing.T) {
	var buf bytes.Buffer
	core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(&buf), zapcore.DebugLevel)
	ctx := ctxzap.ToContext(context.Background(), zap.New(core))
	client := newMembershipServer(t, 200, map[string][]membershipRow{"sys_user_grmember": {{"row01", ""}, {"row02", ""}, {"row03", "u1"}}})

	drainGrants(ctx, t, groupGrants(client), testGroup(t))

	logs := buf.String()
	require.Equal(t, 1, strings.Count(logs, "skipping membership rows with empty principal reference"))
	require.Contains(t, logs, `"table":"sys_user_grmember"`)
	require.Contains(t, logs, `"resource_id":"G"`)
	require.Contains(t, logs, `"skipped_count":2`)
	require.Contains(t, logs, `"total_occurrences":1`)
	require.Contains(t, logs, `"level":"debug"`)
	require.NotContains(t, logs, `"level":"warn"`)
	require.NotContains(t, logs, `"level":"error"`)
}

// One builder serves every Grants call in a sync, so its counter spans resources.
func TestGrants_SamplesSkipLogsAcrossResources(t *testing.T) {
	var buf bytes.Buffer
	core := zapcore.NewCore(zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()), zapcore.AddSync(&buf), zapcore.DebugLevel)
	ctx := ctxzap.ToContext(context.Background(), zap.New(core))
	client := newMembershipServer(t, 200, map[string][]membershipRow{"sys_user_grmember": {{"row01", ""}, {"row02", "u1"}}})

	builder := groupBuilder(client)
	for i := 0; i < 12; i++ {
		drainGrants(ctx, t, builder.Grants, testGroup(t))
	}

	logs := buf.String()
	require.Equal(t, 2, strings.Count(logs, "skipping membership rows with empty principal reference"), "expected occurrences 1 and 10 out of 12")
	require.Contains(t, logs, `"total_occurrences":1}`)
	require.Contains(t, logs, `"total_occurrences":10}`)
}

func TestMapGroupMembers_DropsEmptyReferences(t *testing.T) {
	members, skipped := mapGroupMembers([]servicenow.GroupMember{{User: "u1"}, {User: ""}, {User: "\t"}, {User: "u2"}})

	require.Equal(t, []string{"u1", "u2"}, members)
	require.Equal(t, 2, skipped)
}

// TestGrants_PebbleStoreAcceptsGrants stores the connector's grants through
// the SDK's pebble engine, which rejects any grant with an empty principal.
func TestGrants_PebbleStoreAcceptsGrants(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := dotc1z.NewStore(ctx, filepath.Join(dir, "sync.c1z"), dotc1z.WithEngine(c1zstore.EnginePebble), dotc1z.WithTmpDir(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })

	_, err = store.StartNewSync(ctx, connectorstore.SyncTypeFull, "")
	require.NoError(t, err)

	role := testRole(t)
	group := testGroup(t)
	require.NoError(t, store.PutResourceTypes(ctx, resourceTypeUser, resourceTypeGroup, resourceTypeRole))
	require.NoError(t, store.PutResources(ctx, role, group))
	require.NoError(t, store.PutEntitlements(ctx,
		ent.NewAssignmentEntitlement(role, roleMembership),
		ent.NewAssignmentEntitlement(group, groupMembership),
	))

	// The shape of grant emitted before empty references were skipped.
	emptyPrincipal := grant.NewGrant(role, roleMembership, &v2.ResourceId{ResourceType: resourceTypeUser.Id, Resource: ""})
	require.ErrorContains(t, store.PutGrants(ctx, emptyPrincipal), "missing principal")

	client := newMembershipServer(t, 200, map[string][]membershipRow{
		"sys_user_has_role":  {{"row01", "u1"}, {"row02", ""}, {"row03", ""}},
		"sys_group_has_role": {{"row04", ""}, {"row05", "g1"}},
		"sys_user_grmember":  {{"row06", " "}, {"row07", "u1"}},
	})
	roleGrantsOut, _ := drainGrants(ctx, t, roleGrants(client), role)
	groupGrantsOut, _ := drainGrants(ctx, t, groupGrants(client), group)

	require.NoError(t, store.PutGrants(ctx, append(roleGrantsOut, groupGrantsOut...)...))
	require.NoError(t, store.EndSync(ctx))
}
