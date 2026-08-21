package servicenow

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
)

// aclStub emulates the Table API: sysparm_limit and sysparm_offset pick a window
// before row-level ACLs run, and X-Total-Count counts matching rows without them.
type aclStub[T any] struct {
	ids            []string        // every row in the table, ascending
	hidden         map[string]bool // rows the sync account may not read
	newRow         func(id string) T
	omitTotalCount bool // an instance that never sends the header
	// Models an instance whose X-Total-Count is computed after ACL filtering.
	reportPostACLCount bool
	requests           int
	seen               []stubRequest
}

type stubRequest struct {
	query  string
	limit  string
	offset string
}

func (s *aclStub[T]) handler(t *testing.T) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		s.requests++
		s.seen = append(s.seen, stubRequest{
			query:  q.Get("sysparm_query"),
			limit:  q.Get("sysparm_limit"),
			offset: q.Get("sysparm_offset"),
		})

		cursor := ""
		if raw := q.Get("sysparm_query"); strings.Contains(raw, "sys_id>") {
			rest := raw[strings.Index(raw, "sys_id>")+len("sys_id>"):]
			cursor = strings.SplitN(rest, "^", 2)[0]
		}

		past := make([]string, 0, len(s.ids))
		for _, id := range s.ids {
			if id > cursor {
				past = append(past, id)
			}
		}

		limit, _ := strconv.Atoi(q.Get("sysparm_limit"))
		offset, _ := strconv.Atoi(q.Get("sysparm_offset"))

		// The window is cut before ACLs run.
		window := past
		if offset < len(window) {
			window = window[offset:]
		} else {
			window = nil
		}
		if limit > 0 && limit < len(window) {
			window = window[:limit]
		}

		rows := make([]T, 0, len(window))
		for _, id := range window {
			if !s.hidden[id] {
				rows = append(rows, s.newRow(id))
			}
		}

		if !s.omitTotalCount {
			count := len(past) // the COUNT(*): matching rows, ACLs not applied
			switch {
			case s.reportPostACLCount:
				count = len(rows)
			case q.Get("sysparm_no_count") == "true":
				// Modelled so reintroducing the parameter fails the ACL tests
				// instead of silently reverting the fix: the window size cannot
				// tell an emptied window from the end of the table.
				count = len(window)
			}
			w.Header().Set("X-Total-Count", strconv.Itoa(count))
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(ListResponse[T]{Result: rows}); err != nil {
			t.Errorf("failed to encode test response: %v", err)
		}
	}
}

// sysID renders n as a 32-hex sys_id, zero-padded so string order matches n. Real
// sys_ids are needed because the token codec validates their shape.
func sysID(n int) string {
	return fmt.Sprintf("%032x", n)
}

// sysIDs is sysID over a range, inclusive.
func sysIDs(from, to int) []string {
	ids := make([]string, 0, to-from+1)
	for n := from; n <= to; n++ {
		ids = append(ids, sysID(n))
	}
	return ids
}

// newACLStub builds a stub for any row type; T comes from newRow.
func newACLStub[T any](newRow func(id string) T, ids []string, hidden ...string) *aclStub[T] {
	h := make(map[string]bool, len(hidden))
	for _, id := range hidden {
		h[id] = true
	}
	return &aclStub[T]{ids: ids, hidden: h, newRow: newRow}
}

func roleStub(ids []string, hidden ...string) *aclStub[Role] {
	return newACLStub(func(id string) Role { return Role{BaseResource: BaseResource{Id: id}} }, ids, hidden...)
}

func userToRoleStub(ids []string, hidden ...string) *aclStub[UserToRole] {
	return newACLStub(func(id string) UserToRole { return UserToRole{BaseResource: BaseResource{Id: id}} }, ids, hidden...)
}

func newStubClient[T any](t *testing.T, stub *aclStub[T], domains []string) (*Client, func()) {
	t.Helper()
	server := httptest.NewServer(stub.handler(t))
	client, err := NewClient(uhttp.NewBaseHttpClient(server.Client()), "Basic dGVzdDp0ZXN0", "dev0", nil, domains, nil, server.URL)
	if err != nil {
		server.Close()
		t.Fatalf("unexpected error creating client: %v", err)
	}
	return client, server.Close
}

// drain walks a listing the way the SDK does, feeding each token back in. Guards
// against a pagination bug that repeats a token or never terminates.
func drain[T any](t *testing.T, bound int, step func(KeysetPaginationVars) ([]T, string, error)) []T {
	t.Helper()

	var got []T
	token := ""
	for range bound {
		lastID, offset, err := ParseKeysetToken(token)
		if err != nil {
			t.Fatalf("connector returned an unparseable token %q: %v", token, err)
		}

		page, next, err := step(KeysetPaginationVars{LastID: lastID, Offset: offset})
		if err != nil {
			t.Fatalf("unexpected error on token %q: %v", token, err)
		}
		got = append(got, page...)
		if next == "" {
			return got
		}
		if next == token {
			t.Fatalf("token %q repeated -- pagination would loop forever", next)
		}
		token = next
	}
	t.Fatalf("pagination did not terminate within %d pages", bound)
	return nil
}

// drainRoles drains a role listing and returns the sys_ids collected.
func drainRoles(t *testing.T, client *Client, limit int, bound int) []string {
	t.Helper()

	rows := drain(t, bound, func(page KeysetPaginationVars) ([]Role, string, error) {
		page.Limit = limit
		rows, next, _, err := client.GetRoles(context.Background(), page)
		return rows, next, err
	})

	ids := make([]string, 0, len(rows))
	for _, r := range rows {
		ids = append(ids, r.Id)
	}
	return ids
}

// A whole window hidden by row-level read ACLs must not end the listing while
// readable rows remain past it.
func TestGetRoles_StepsPastACLEmptiedWindow(t *testing.T) {
	// Limit 2, so the window 3..4 is emptied in full. Limit 1 would be a
	// point lookup, which deliberately skips all of this.
	stub := roleStub(sysIDs(1, 6), sysID(3), sysID(4))
	client, done := newStubClient(t, stub, nil)
	defer done()

	got := drainRoles(t, client, 2, 20)

	want := []string{sysID(1), sysID(2), sysID(5), sysID(6)}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("rows synced = %v, want %v (the listing must continue past a window emptied by row-level ACLs)", got, want)
	}
}

// A run of unreadable rows longer than one window: the skip advances more than once.
func TestGetRoles_StepsPastLongACLEmptiedRun(t *testing.T) {
	stub := roleStub(sysIDs(1, 8), sysID(3), sysID(4), sysID(5), sysID(6))
	client, done := newStubClient(t, stub, nil)
	defer done()

	got := drainRoles(t, client, 2, 20)

	want := []string{sysID(1), sysID(2), sysID(7), sysID(8)}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("rows synced = %v, want %v", got, want)
	}

	// Every request is a page fetch: no request exists purely to ask for a count.
	for i, req := range stub.seen {
		if req.limit == "1" {
			t.Errorf("request %d has limit 1: a separate count probe should not exist", i)
		}
	}
}

// The normal path: an empty page past the last row ends the listing.
func TestGetRoles_TerminatesOnGenuineEnd(t *testing.T) {
	stub := roleStub(sysIDs(1, 2))
	client, done := newStubClient(t, stub, nil)
	defer done()

	got := drainRoles(t, client, 2, 10)

	if strings.Join(got, ",") != strings.Join(sysIDs(1, 2), ",") {
		t.Errorf("rows synced = %v, want the two readable rows", got)
	}
	if stub.requests != 2 {
		t.Errorf("requests = %d, want 2 (one page, one terminator)", stub.requests)
	}
}

// The tail case: every remaining row unreadable must end cleanly, not fail a sync
// that already collected everything it is allowed to see.
func TestGetRoles_TerminatesWhenEveryRemainingRowIsUnreadable(t *testing.T) {
	stub := roleStub(sysIDs(1, 3), sysID(2), sysID(3))
	client, done := newStubClient(t, stub, nil)
	defer done()

	got := drainRoles(t, client, 2, 10)

	if strings.Join(got, ",") != sysID(1) {
		t.Errorf("rows synced = %v, want only the first row (the rest are unreadable)", got)
	}
}

// A deployment that never sends X-Total-Count ends the listing instead of stepping
// blindly, which has no bound. It can under-report, but never fails a sync nor loops.
func TestGetRoles_EndsListingWhenRowCountUnavailable(t *testing.T) {
	stub := roleStub(sysIDs(1, 8), sysID(3), sysID(4))
	stub.omitTotalCount = true
	client, done := newStubClient(t, stub, nil)
	defer done()

	got := drainRoles(t, client, 2, 20)

	if strings.Join(got, ",") != strings.Join(sysIDs(1, 2), ",") {
		t.Errorf("rows synced = %v, want the rows before the hidden block", got)
	}
}

// A page that returned rows must never yield an empty cursor: the SDK would read
// it as the end of the listing and drop the rest of the table.
func TestGetRoles_ErrorsWhenAPageOfRowsYieldsNoCursor(t *testing.T) {
	stub := newACLStub(func(string) Role { return Role{} }, sysIDs(1, 4))
	client, done := newStubClient(t, stub, nil)
	defer done()

	rows, next, _, err := client.GetRoles(context.Background(), KeysetPaginationVars{Limit: 2})
	if err == nil {
		t.Fatalf("rows = %d, next = %q, err = nil; want an error rather than a silent end of listing", len(rows), next)
	}
}

// An unparseable X-Total-Count ends the listing rather than failing the sync. The
// header-absent case takes the same branch; this one covers the parse error.
func TestGetRoles_EndsListingWhenRowCountIsNotNumeric(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Total-Count", "not-a-number")
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(ListResponse[Role]{Result: nil}); err != nil {
			t.Errorf("failed to encode test response: %v", err)
		}
	}))
	defer server.Close()

	client, err := NewClient(uhttp.NewBaseHttpClient(server.Client()), "Basic dGVzdDp0ZXN0", "dev0", nil, nil, nil, server.URL)
	if err != nil {
		t.Fatalf("unexpected error creating client: %v", err)
	}

	rows, next, _, err := client.GetRoles(context.Background(), KeysetPaginationVars{Limit: 200})
	if err != nil {
		t.Fatalf("unexpected error: %v (a header we cannot parse must not fail the sync)", err)
	}
	if len(rows) != 0 || next != "" {
		t.Errorf("rows = %d, next = %q; want the listing to end", len(rows), next)
	}
}

// The skip is bounded by the row count, so a hidden tail cannot walk past the table.
func TestGetRoles_StopsSteppingAtTheRowCount(t *testing.T) {
	ids := sysIDs(1, 8)
	stub := roleStub(ids, ids...) // every row unreadable
	client, done := newStubClient(t, stub, nil)
	defer done()

	got := drainRoles(t, client, 2, 10)

	if len(got) != 0 {
		t.Errorf("rows synced = %v, want none (every row is unreadable)", got)
	}
	// 8 rows at limit 2: windows at offset 0,2,4,6 and then the count stops it.
	if stub.requests > 5 {
		t.Errorf("requests = %d, want at most 5: the row count must bound the walk", stub.requests)
	}
}

// A seek page that returns rows costs one request and no follow-up.
func TestGetRoles_ProductivePageCostsOneRequest(t *testing.T) {
	stub := roleStub(sysIDs(1, 3))
	client, done := newStubClient(t, stub, nil)
	defer done()

	if _, _, _, err := client.GetRoles(context.Background(), KeysetPaginationVars{Limit: 2, LastID: sysID(1)}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stub.requests != 1 {
		t.Fatalf("requests = %d, want 1 (a page that returns rows needs no follow-up)", stub.requests)
	}
	if got := stub.seen[0].query; !strings.Contains(got, "sys_id>"+sysID(1)) {
		t.Errorf("query = %q, want the seek condition", got)
	}
}

// A listing with no rows costs one request: the count already says zero.
func TestGetRoles_EmptyListingCostsOneRequest(t *testing.T) {
	stub := roleStub(nil)
	client, done := newStubClient(t, stub, nil)
	defer done()

	_, next, _, err := client.GetRoles(context.Background(), KeysetPaginationVars{Limit: 200})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if next != "" {
		t.Errorf("next token = %q, want empty for an empty listing", next)
	}
	if stub.requests != 1 {
		t.Errorf("requests = %d, want 1", stub.requests)
	}
}

// Provisioning existence checks (Limit 1) must not step: empty is the answer.
func TestGetUserToRole_PointLookupMakesOneRequest(t *testing.T) {
	stub := userToRoleStub(nil)
	client, done := newStubClient(t, stub, nil)
	defer done()

	rows, _, _, err := client.GetUserToRole(context.Background(), "user-1", "role-1", KeysetPaginationVars{Limit: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("rows = %v, want none", rows)
	}
	if stub.requests != 1 {
		t.Fatalf("requests = %d, want 1 (an existence check must not step)", stub.requests)
	}
	if got := stub.seen[0].offset; got != "" && got != "0" {
		t.Errorf("sysparm_offset = %q, want unset for a point lookup", got)
	}
}

// A point lookup must never escalate: it has no page to continue.
func TestGetUserToRole_PointLookupNeverEscalates(t *testing.T) {
	stub := userToRoleStub(sysIDs(1, 4), sysID(1), sysID(2), sysID(3), sysID(4))
	client, done := newStubClient(t, stub, nil)
	defer done()

	rows, next, _, err := client.GetUserToRole(context.Background(), "user-1", "role-1", KeysetPaginationVars{Limit: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 0 {
		t.Errorf("rows = %v, want none", rows)
	}
	if next != "" {
		t.Errorf("next token = %q, want empty: an existence check has no page to continue", next)
	}
	if stub.requests != 1 {
		t.Errorf("requests = %d, want 1", stub.requests)
	}
}

// A point lookup (Limit:1) must never fail on a matched row's sys_id, even
// one that couldn't be encoded as a cursor: the token is never used for a
// call this shape, and failing here would turn a successful Grant/Revoke
// idempotency check ("this grant already exists") into a hard error instead
// of the GrantAlreadyExists/GrantAlreadyRevoked annotation callers rely on.
func TestGetUserToRole_PointLookupIgnoresUnencodableSysID(t *testing.T) {
	stub := userToRoleStub([]string{"bad^id"})
	client, done := newStubClient(t, stub, nil)
	defer done()

	rows, next, _, err := client.GetUserToRole(context.Background(), "user-1", "role-1", KeysetPaginationVars{Limit: 1})
	if err != nil {
		t.Fatalf("unexpected error: %v (a point lookup must not encode a cursor for its matched row)", err)
	}
	if len(rows) != 1 {
		t.Errorf("rows = %v, want the one matched row", rows)
	}
	if next != "" {
		t.Errorf("next token = %q, want empty: a point lookup has no page to continue", next)
	}
}

// The allowed-domains filter must survive every request of a listing, skip included:
// dropping it there would recover rows scoped differently from the page it replaces.
func TestGetUserToRole_DomainFilterSurvivesTheSkip(t *testing.T) {
	stub := userToRoleStub(sysIDs(1, 5), sysID(3), sysID(4))
	client, done := newStubClient(t, stub, []string{"example.com"})
	defer done()

	rows := drain(t, 20, func(page KeysetPaginationVars) ([]UserToRole, string, error) {
		page.Limit = 2
		rows, next, _, err := client.GetUserToRole(context.Background(), "", "role-1", page)
		return rows, next, err
	})
	got := make([]string, 0, len(rows))
	for _, r := range rows {
		got = append(got, r.Id)
	}

	if strings.Join(got, ",") != strings.Join([]string{sysID(1), sysID(2), sysID(5)}, ",") {
		t.Errorf("rows synced = %v, want rows 1, 2 and 5", got)
	}

	sawSkipOffset := false
	for i, req := range stub.seen {
		if !strings.Contains(req.query, "user.emailENDSWITH@example.com") {
			t.Errorf("request %d query = %q, want the allowed-domains filter on every request", i, req.query)
		}
		if !strings.Contains(req.query, "role=role-1") {
			t.Errorf("request %d query = %q, want the role scope on every request", i, req.query)
		}
		if !strings.Contains(req.query, "ORDERBYsys_id") {
			t.Errorf("request %d query = %q, want ORDERBYsys_id on every request", i, req.query)
		}
		if req.offset != "" && req.offset != "0" {
			sawSkipOffset = true
		}
	}
	if !sawSkipOffset {
		t.Error("no request carried a skip offset -- the ACL-emptied window was not stepped past")
	}
}

// The domain filter must survive a skip that overrides the capped page size.
func TestGetUserToRole_DomainFilterCapsPageSizeOnSkip(t *testing.T) {
	ids := sysIDs(1, 300)
	stub := userToRoleStub(ids, ids...)
	client, done := newStubClient(t, stub, []string{"example.com"})
	defer done()

	// Enumeration (empty userId) with allowed-domains set: the cap applies.
	_, next, _, err := client.GetUserToRole(context.Background(), "", "role-1", KeysetPaginationVars{
		Limit:  200,
		LastID: sysID(1),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if next == "" {
		t.Fatal("expected the listing to continue: rows remain past the cursor")
	}

	want := strconv.Itoa(domainFilteredPageSize)
	for i, req := range stub.seen {
		if req.limit != want {
			t.Errorf("request %d sysparm_limit = %q, want the domain cap %q", i, req.limit, want)
		}
		if !strings.Contains(req.query, "user.emailENDSWITH@example.com") {
			t.Errorf("request %d lost the allowed-domains filter: %q", i, req.query)
		}
	}
}

// One request per call is what makes every step of a skip survive a restart.
func TestGetRoles_OneRequestPerCall(t *testing.T) {
	stub := roleStub(sysIDs(1, 8), sysID(3), sysID(4), sysID(5), sysID(6))
	client, done := newStubClient(t, stub, nil)
	defer done()

	calls := 0
	drain(t, 20, func(page KeysetPaginationVars) ([]Role, string, error) {
		page.Limit = 2
		before := stub.requests
		rows, next, _, err := client.GetRoles(context.Background(), page)
		if got := stub.requests - before; got != 1 {
			t.Fatalf("call %d issued %d requests, want exactly 1", calls, got)
		}
		calls++
		return rows, next, err
	})
}

// A non-positive Limit would silently discard every row and end the listing.
func TestGetRoles_RejectsNonPositiveLimit(t *testing.T) {
	stub := roleStub(sysIDs(1, 4))
	client, done := newStubClient(t, stub, nil)
	defer done()

	for _, limit := range []int{0, -1} {
		if _, _, _, err := client.GetRoles(context.Background(), KeysetPaginationVars{Limit: limit}); err == nil {
			t.Errorf("Limit %d: expected an error", limit)
		}
	}
}

// The empty-page branch returns before the result is built, so it is the one place
// a lost rate-limit annotation would compile and pass every other test.
func TestGetRoles_EmptyPageStillCarriesRateLimitAnnotations(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-RateLimit-Limit", "100")
		w.Header().Set("X-RateLimit-Remaining", "3")
		w.Header().Set("X-RateLimit-Reset", "30")
		w.Header().Set("X-Total-Count", "0")
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(ListResponse[Role]{Result: nil}); err != nil {
			t.Errorf("failed to encode test response: %v", err)
		}
	}))
	defer server.Close()

	client, err := NewClient(uhttp.NewBaseHttpClient(server.Client()), "Basic dGVzdDp0ZXN0", "dev0", nil, nil, nil, server.URL)
	if err != nil {
		t.Fatalf("unexpected error creating client: %v", err)
	}

	rows, next, annos, err := client.GetRoles(context.Background(), KeysetPaginationVars{Limit: 50})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rows) != 0 || next != "" {
		t.Fatalf("rows = %d, next = %q; want an empty terminating page", len(rows), next)
	}

	rl := &v2.RateLimitDescription{}
	if ok, err := annos.Pick(rl); err != nil || !ok {
		t.Fatalf("no RateLimitDescription on an empty page (ok=%v, err=%v); the SDK limiter loses pacing data on every listing terminator", ok, err)
	}
	if rl.GetRemaining() != 3 {
		t.Errorf("remaining = %d, want 3", rl.GetRemaining())
	}
}

// A post-ACL X-Total-Count makes an emptied window indistinguishable from the end
// of the table, so the listing ends there. Documents the signal's limit.
func TestGetRoles_UnderReportsWhenCountIsPostACL(t *testing.T) {
	stub := roleStub(sysIDs(1, 6), sysID(3), sysID(4))
	stub.reportPostACLCount = true
	client, done := newStubClient(t, stub, nil)
	defer done()

	got := drainRoles(t, client, 2, 20)

	if strings.Join(got, ",") != strings.Join(sysIDs(1, 2), ",") {
		t.Errorf("rows synced = %v; a post-ACL count ends the listing at the first emptied window", got)
	}
}
