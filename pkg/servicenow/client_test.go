package servicenow

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestGetRoles_DoesNotTruncateOnShortNonEmptyPage guards against treating a
// short-but-nonempty page as the last one. ServiceNow doesn't reliably
// honor sysparm_limit's exact row count, so terminating on "page shorter
// than requested" silently truncates the listing -- only a genuinely empty
// page may end pagination.
func TestGetRoles_DoesNotTruncateOnShortNonEmptyPage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		isSeeking := strings.Contains(r.URL.Query().Get("sysparm_query"), "sys_id>")

		// Page 1: full. Page 2: ServiceNow under-delivers (3 of 50
		// requested) even though more data remains -- must NOT read as
		// last page. Page 3: genuinely empty, the only valid termination
		// signal.
		var n int
		switch {
		case !isSeeking:
			n = 50
		case strings.Contains(r.URL.Query().Get("sysparm_query"), "role-049"):
			n = 3
		default:
			n = 0
		}

		roles := make([]Role, n)
		for i := range roles {
			roles[i] = Role{BaseResource: BaseResource{Id: fmt.Sprintf("role-%03d", i)}}
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(ListResponse[Role]{Result: roles}); err != nil {
			// t.Fatalf must not be called from the handler's goroutine --
			// it would only stop this goroutine mid-response and surface a
			// confusing secondary failure in the test itself.
			t.Errorf("failed to encode test response: %v", err)
		}
	}))
	defer server.Close()

	client, err := NewClient(uhttp.NewBaseHttpClient(server.Client()), "Basic dGVzdDp0ZXN0", "dev0", nil, nil, nil, server.URL)
	if err != nil {
		t.Fatalf("unexpected error creating client: %v", err)
	}

	page1, next1, _, err := client.GetRoles(context.Background(), KeysetPaginationVars{Limit: 50})
	if err != nil {
		t.Fatalf("unexpected error on page 1: %v", err)
	}
	if len(page1) != 50 {
		t.Errorf("page1 len = %d, want 50", len(page1))
	}
	if next1 == "" {
		t.Fatalf("expected a non-empty next token after a full page")
	}

	page2, next2, _, err := client.GetRoles(context.Background(), KeysetPaginationVars{Limit: 50, LastID: next1})
	if err != nil {
		t.Fatalf("unexpected error on page 2: %v", err)
	}
	if len(page2) != 3 {
		t.Errorf("page2 len = %d, want 3", len(page2))
	}
	if next2 == "" {
		t.Fatalf("pagination stopped after a short-but-nonempty page (3 rows) while more rows remained")
	}

	page3, next3, _, err := client.GetRoles(context.Background(), KeysetPaginationVars{Limit: 50, LastID: next2})
	if err != nil {
		t.Fatalf("unexpected error on page 3: %v", err)
	}
	if len(page3) != 0 {
		t.Errorf("page3 len = %d, want 0", len(page3))
	}
	if next3 != "" {
		t.Errorf("expected pagination to terminate after a genuinely empty page, got token %q", next3)
	}
}

// A keyset page must not fail over the legacy offset token, which keyset callers
// never compute (see doRequestWithRetryKeyset). They do read X-Total-Count, but
// only on an empty page, and an unparseable value there just ends the listing.
func TestGetRoles_IgnoresMalformedLegacyPaginationHeaders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// legacyOffsetToken would fail parsing this as an int; the keyset
		// path must never even attempt to.
		w.Header().Set("X-Total-Count", "not-a-number")
		w.Header().Set("Content-Type", "application/json")

		roles := []Role{{BaseResource: BaseResource{Id: "role-000"}}}
		if err := json.NewEncoder(w).Encode(ListResponse[Role]{Result: roles}); err != nil {
			t.Errorf("failed to encode test response: %v", err)
		}
	}))
	defer server.Close()

	client, err := NewClient(uhttp.NewBaseHttpClient(server.Client()), "Basic dGVzdDp0ZXN0", "dev0", nil, nil, nil, server.URL)
	if err != nil {
		t.Fatalf("unexpected error creating client: %v", err)
	}

	roles, _, _, err := client.GetRoles(context.Background(), KeysetPaginationVars{Limit: 50})
	if err != nil {
		t.Fatalf("unexpected error: %v (a malformed legacy pagination header must not fail a keyset page fetch)", err)
	}
	if len(roles) != 1 || roles[0].Id != "role-000" {
		t.Errorf("roles = %+v, want a single role-000", roles)
	}
}

// TestGetRoles_StepsPastACaretSysIdByOffset guards a sys_id containing ^
// (ServiceNow's unescapable AND operator) sorting last in a page, which can
// never be embedded in the next page's sysparm_query seek fragment. Before
// this fix that permanently failed the sync; now it steps past the window
// by sysparm_offset instead, the same fallback used for an ACL-emptied
// window.
func TestGetRoles_StepsPastACaretSysIdByOffset(t *testing.T) {
	var page2Query, page2Offset string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		isSeeking := strings.Contains(r.URL.Query().Get("sysparm_query"), "sys_id>")

		var roles []Role
		switch {
		case !isSeeking && r.URL.Query().Get("sysparm_offset") == "":
			roles = []Role{
				{BaseResource: BaseResource{Id: "role-000"}},
				{BaseResource: BaseResource{Id: "qa^cxp947caret"}},
			}
		default:
			page2Query = r.URL.Query().Get("sysparm_query")
			page2Offset = r.URL.Query().Get("sysparm_offset")
			roles = nil
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(ListResponse[Role]{Result: roles}); err != nil {
			t.Errorf("failed to encode test response: %v", err)
		}
	}))
	defer server.Close()

	client, err := NewClient(uhttp.NewBaseHttpClient(server.Client()), "Basic dGVzdDp0ZXN0", "dev0", nil, nil, nil, server.URL)
	if err != nil {
		t.Fatalf("unexpected error creating client: %v", err)
	}

	page1, next1, _, err := client.GetRoles(context.Background(), KeysetPaginationVars{Limit: 50})
	if err != nil {
		t.Fatalf("unexpected error on page 1: %v (a caret-bearing sys_id sorting last must not fail the sync)", err)
	}
	if len(page1) != 2 {
		t.Fatalf("page1 len = %d, want 2", len(page1))
	}
	if next1 == "" {
		t.Fatalf("expected a non-empty next token after a full page")
	}

	lastID, offset, err := ParseKeysetToken(next1)
	if err != nil {
		t.Fatalf("ParseKeysetToken(%q) returned an error: %v", next1, err)
	}
	if lastID != "" || offset != 2 {
		t.Errorf("ParseKeysetToken(%q) = (%q, %d), want (\"\", 2): step past the whole window by offset, not seek on the caret sys_id", next1, lastID, offset)
	}

	_, _, _, err = client.GetRoles(context.Background(), KeysetPaginationVars{Limit: 50, LastID: lastID, Offset: offset})
	if err != nil {
		t.Fatalf("unexpected error on page 2: %v", err)
	}
	if strings.Contains(page2Query, "sys_id>") {
		t.Errorf("page 2 sysparm_query = %q, must not seek on the caret sys_id", page2Query)
	}
	if page2Offset != "2" {
		t.Errorf("page 2 sysparm_offset = %q, want %q", page2Offset, "2")
	}
}

// TestGetUsers_CapsPageSizeWhenDomainFilterApplies covers GetUsers
// specifically: it always enumerates (no per-user provisioning variant),
// so the domain filter's page-size cap always applies when AllowedDomains
// is configured.
func TestGetUsers_CapsPageSizeWhenDomainFilterApplies(t *testing.T) {
	var gotLimit string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotLimit = r.URL.Query().Get("sysparm_limit")
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(ListResponse[User]{Result: nil}); err != nil {
			t.Errorf("failed to encode test response: %v", err)
		}
	}))
	defer server.Close()

	client, err := NewClient(uhttp.NewBaseHttpClient(server.Client()), "Basic dGVzdDp0ZXN0", "dev0", nil, []string{"example.com"}, nil, server.URL)
	if err != nil {
		t.Fatalf("unexpected error creating client: %v", err)
	}

	_, _, _, err = client.GetUsers(context.Background(), KeysetPaginationVars{Limit: 200})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := fmt.Sprintf("%d", domainFilteredPageSize)
	if gotLimit != want {
		t.Errorf("sysparm_limit sent = %q, want %q (AllowedDomains configured must cap the page size)", gotLimit, want)
	}
}

// TestNonNumericRateLimitHeaderDoesNotFailA200 is a regression guard. uhttp
// returns (resp[200], optionError) when a DoOption fails, and the only option
// we pass is rate-limit extraction -- which runs strconv.ParseInt over the
// limit/remaining headers, so a non-numeric value errors. Treating that as
// fatal discarded a successfully fetched page over a header we only use for
// pacing. Verified against a live instance: ServiceNow's Table API sends no
// rate-limit headers at all on a 200, so this is reachable only once an
// instance has a rate-limit rule configured -- which is exactly when we least
// want the sync to fall over.
func TestNonNumericRateLimitHeaderDoesNotFailA200(t *testing.T) {
	for _, hv := range []string{"unlimited", "n/a", ""} {
		t.Run("limit="+hv, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if hv != "" {
					w.Header().Set("X-RateLimit-Limit", hv)
				}
				w.Header().Set("Content-Type", "application/json")
				if err := json.NewEncoder(w).Encode(ListResponse[Role]{Result: []Role{{BaseResource: BaseResource{Id: "role-000"}}}}); err != nil {
					t.Errorf("failed to encode test response: %v", err)
				}
			}))
			defer server.Close()

			client, err := NewClient(uhttp.NewBaseHttpClient(server.Client()), "Basic dGVzdDp0ZXN0", "dev0", nil, nil, nil, server.URL)
			if err != nil {
				t.Fatalf("unexpected error creating client: %v", err)
			}

			roles, _, _, err := client.GetRoles(context.Background(), KeysetPaginationVars{Limit: 50})
			if err != nil {
				t.Fatalf("an advisory rate-limit header must not fail the page: %v", err)
			}
			if len(roles) != 1 {
				t.Errorf("roles len = %d, want 1 (the page must survive)", len(roles))
			}
		})
	}
}

// TestWritesCarryRateLimitAnnotations covers the verbs behind provisioning.
// post/patch/delete used to return only error, which structurally blocked every
// grant, revoke, and account mutation from reporting rate-limit data.
func TestWritesCarryRateLimitAnnotations(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-RateLimit-Limit", "100")
		w.Header().Set("X-RateLimit-Remaining", "3")
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(SingleResponse[BaseResource]{Result: BaseResource{Id: "x"}}); err != nil {
			t.Errorf("failed to encode test response: %v", err)
		}
	}))
	defer server.Close()

	client, err := NewClient(uhttp.NewBaseHttpClient(server.Client()), "Basic dGVzdDp0ZXN0", "dev0", nil, nil, nil, server.URL)
	if err != nil {
		t.Fatalf("unexpected error creating client: %v", err)
	}

	cases := map[string]func() (annotations.Annotations, error){
		"AddUserToGroup (post)": func() (annotations.Annotations, error) {
			return client.AddUserToGroup(context.Background(), GroupMemberPayload{User: "u", Group: "g"})
		},
		"RemoveUserFromGroup (delete)": func() (annotations.Annotations, error) { return client.RemoveUserFromGroup(context.Background(), "id") },
		"GrantRoleToUser (post)": func() (annotations.Annotations, error) {
			return client.GrantRoleToUser(context.Background(), UserToRolePayload{User: "u", Role: "r"})
		},
		"RevokeRoleFromUser (delete)": func() (annotations.Annotations, error) { return client.RevokeRoleFromUser(context.Background(), "id") },
	}

	for name, call := range cases {
		annos, err := call()
		if err != nil {
			t.Errorf("%s: unexpected error: %v", name, err)
			continue
		}
		rl := &v2.RateLimitDescription{}
		if ok, pErr := annos.Pick(rl); pErr != nil || !ok {
			t.Errorf("%s: no RateLimitDescription annotation (ok=%v err=%v)", name, ok, pErr)
			continue
		}
		if rl.GetRemaining() != 3 {
			t.Errorf("%s: remaining = %d, want 3", name, rl.GetRemaining())
		}
	}
}

// TestNonJSONResponseNamesTheActualProblem covers the shape a hibernating
// ServiceNow PDI returns: HTTP 200 with an HTML page. Decoding that as JSON
// produces "invalid character '<' looking for beginning of value", which sent
// two engineers looking for a connector bug when the instance was simply
// asleep. The error must say so.
func TestNonJSONResponseNamesTheActualProblem(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=UTF-8")
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("<!DOCTYPE html><html><body>This instance is hibernating</body></html>")); err != nil {
			t.Errorf("failed to write test response: %v", err)
		}
	}))
	defer server.Close()

	client, err := NewClient(uhttp.NewBaseHttpClient(server.Client()), "Basic dGVzdDp0ZXN0", "dev0", nil, nil, nil, server.URL)
	if err != nil {
		t.Fatalf("unexpected error creating client: %v", err)
	}

	_, _, _, err = client.GetRoles(context.Background(), KeysetPaginationVars{Limit: 50})
	if err == nil {
		t.Fatalf("expected an error decoding an HTML body")
	}

	msg := err.Error()
	for _, want := range []string{"hibernating", "text/html", "status 200"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error message missing %q; got: %s", want, msg)
		}
	}
}

// TestNewClientValidatesBaseURL covers both URL shapes the constructor
// produces: the override is an absolute URL spliced in by apiURL, while the
// deployment-derived value is a bare host that ticket.go composes into
// url.URL{Scheme: "https", Host: ...}. Either being malformed used to surface
// only as a confusing failure on the first request.
func TestNewClientValidatesBaseURL(t *testing.T) {
	cases := []struct {
		name       string
		deployment string
		override   string
		wantErr    bool
	}{
		{"valid override", "dev0", "https://example.service-now.com", false},
		{"valid override with port", "dev0", "http://127.0.0.1:8080", false},
		{"override missing scheme", "dev0", "example.service-now.com", true},
		{"override missing host", "dev0", "https://", true},
		{"valid deployment", "dev0", "", false},
		{"deployment with a space", "dev 0", "", true},
		{"deployment with a slash", "a/b", "", true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewClient(nil, "Basic dGVzdDp0ZXN0", tc.deployment, nil, nil, nil, tc.override)
			if tc.wantErr && err == nil {
				t.Errorf("deployment=%q override=%q: want an error at construction, got nil", tc.deployment, tc.override)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("deployment=%q override=%q: unexpected error %v", tc.deployment, tc.override, err)
			}
		})
	}
}

// TestAuthRetryRecoversFrom401 guards withAuthRetry against the error-plumbing
// change underneath it: the retry only fires if status.Code() still resolves to
// Unauthenticated, which now means resolving *through* uhttp's wrapped error and
// the fmt.Errorf("%w") around it. A regression here is silent -- the sync would
// simply fail on a transient 401 instead of retrying.
func TestAuthRetryRecoversFrom401(t *testing.T) {
	var calls int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if atomic.AddInt32(&calls, 1) == 1 {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if err := json.NewEncoder(w).Encode(ListResponse[Role]{Result: []Role{{BaseResource: BaseResource{Id: "role-000"}}}}); err != nil {
			t.Errorf("failed to encode test response: %v", err)
		}
	}))
	defer server.Close()

	client, err := NewClient(uhttp.NewBaseHttpClient(server.Client()), "Basic dGVzdDp0ZXN0", "dev0", nil, nil, nil, server.URL)
	if err != nil {
		t.Fatalf("unexpected error creating client: %v", err)
	}

	roles, _, _, err := client.GetRoles(context.Background(), KeysetPaginationVars{Limit: 50})
	if err != nil {
		t.Fatalf("expected recovery after a transient 401, got %v", err)
	}
	if got := atomic.LoadInt32(&calls); got != 2 {
		t.Errorf("server saw %d call(s), want 2 (the 401 must be retried once)", got)
	}
	if len(roles) != 1 {
		t.Errorf("roles len = %d, want 1", len(roles))
	}
}

// TestSuccessfulResponseCarriesRateLimitAnnotations covers the half of the
// contract that actually prevents a 429: rate-limit headers on a *successful*
// response must reach the SDK as annotations, so its limiter can pace the
// next request. Reacting only to 429s (the error path) is strictly worse --
// by then the request has already been rejected.
func TestSuccessfulResponseCarriesRateLimitAnnotations(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-RateLimit-Limit", "100")
		w.Header().Set("X-RateLimit-Remaining", "7")
		w.Header().Set("X-RateLimit-Reset", "30")
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(ListResponse[Role]{Result: []Role{{BaseResource: BaseResource{Id: "role-000"}}}}); err != nil {
			t.Errorf("failed to encode test response: %v", err)
		}
	}))
	defer server.Close()

	client, err := NewClient(uhttp.NewBaseHttpClient(server.Client()), "Basic dGVzdDp0ZXN0", "dev0", nil, nil, nil, server.URL)
	if err != nil {
		t.Fatalf("unexpected error creating client: %v", err)
	}

	roles, _, annos, err := client.GetRoles(context.Background(), KeysetPaginationVars{Limit: 50})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(roles) != 1 {
		t.Fatalf("roles len = %d, want 1", len(roles))
	}

	rl := &v2.RateLimitDescription{}
	if ok, err := annos.Pick(rl); err != nil || !ok {
		t.Fatalf("no RateLimitDescription annotation on a successful response (ok=%v, err=%v); the SDK limiter has nothing to pace with", ok, err)
	}
	if rl.GetLimit() != 100 {
		t.Errorf("limit = %d, want 100", rl.GetLimit())
	}
	if rl.GetRemaining() != 7 {
		t.Errorf("remaining = %d, want 7", rl.GetRemaining())
	}
}

// TestRequestsOptOutOfUhttpCache pins a behavioural decision that is invisible
// at the call site: uhttp.BaseHttpClient caches GET 200s in memory for an hour
// by default. Identity and membership listings must be read fresh on every
// sync -- a cached page would resurface grants that have since been revoked.
func TestRequestsOptOutOfUhttpCache(t *testing.T) {
	var gotCacheControl string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotCacheControl = r.Header.Get("Cache-Control")
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

	if _, _, _, err = client.GetRoles(context.Background(), KeysetPaginationVars{Limit: 50}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if gotCacheControl != "no-cache" {
		t.Errorf("Cache-Control = %q, want %q (uhttp would otherwise serve hour-old sync data)", gotCacheControl, "no-cache")
	}
}

// TestErrorCarriesRateLimitDescription guards the contract the SDK retryer
// depends on: a throttled response must surface a v2.RateLimitDescription as
// a gRPC status detail, and it must survive the caller-side fmt.Errorf("%w")
// wrapping every builder applies. Without the detail the retryer falls back
// to blind linear backoff and ignores ServiceNow's Retry-After entirely
// (see baton-sdk pkg/retry/retry.go).
func TestErrorCarriesRateLimitDescription(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "42")
		w.Header().Set("X-RateLimit-Limit", "100")
		w.Header().Set("X-RateLimit-Remaining", "0")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()

	client, err := NewClient(uhttp.NewBaseHttpClient(server.Client()), "Basic dGVzdDp0ZXN0", "dev0", nil, nil, nil, server.URL)
	if err != nil {
		t.Fatalf("unexpected error creating client: %v", err)
	}

	_, _, _, err = client.GetRoles(context.Background(), KeysetPaginationVars{Limit: 50})
	if err == nil {
		t.Fatalf("expected an error on a 429 response")
	}

	// Mirror what the resource builders do to the client's error.
	wrapped := fmt.Errorf("baton-servicenow: failed to list roles: %w", err)

	if code := status.Code(wrapped); code != codes.Unavailable {
		t.Errorf("status code = %v, want %v (the SDK retryer only retries Unavailable/DeadlineExceeded)", code, codes.Unavailable)
	}

	st, ok := status.FromError(wrapped)
	if !ok {
		t.Fatalf("wrapped error did not resolve to a gRPC status")
	}

	var rl *v2.RateLimitDescription
	for _, detail := range st.Details() {
		if d, ok := detail.(*v2.RateLimitDescription); ok {
			rl = d
			break
		}
	}
	if rl == nil {
		t.Fatalf("no RateLimitDescription detail on the error; the SDK retryer will use blind backoff")
	}

	if rl.GetStatus() != v2.RateLimitDescription_STATUS_OVERLIMIT {
		t.Errorf("rate limit status = %v, want STATUS_OVERLIMIT", rl.GetStatus())
	}
	if rl.GetRemaining() != 0 {
		t.Errorf("remaining = %d, want 0", rl.GetRemaining())
	}
	// Retry-After: 42 is a relative offset, so ResetAt lands ~42s out.
	if wait := time.Until(rl.GetResetAt().AsTime()); wait < 30*time.Second || wait > 45*time.Second {
		t.Errorf("ResetAt is %v out, want ~42s (Retry-After must drive the backoff)", wait)
	}
}

// TestErrorCarriesRateLimitDescription_NoHeaders is the case the whole change
// rests on: we have not confirmed that ServiceNow actually emits rate-limit
// headers, and it doesn't matter. ExtractRateLimitData synthesizes
// STATUS_OVERLIMIT + a 60s reset for any bare 429 (baton-sdk
// pkg/ratelimit/http.go), which is what lets the retryer skip its 1s/2s/3s
// ramp. If this stops holding, the change silently loses its value on an
// instance that sends no headers.
func TestErrorCarriesRateLimitDescription_NoHeaders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests) // no rate-limit headers whatsoever
	}))
	defer server.Close()

	client, err := NewClient(uhttp.NewBaseHttpClient(server.Client()), "Basic dGVzdDp0ZXN0", "dev0", nil, nil, nil, server.URL)
	if err != nil {
		t.Fatalf("unexpected error creating client: %v", err)
	}

	_, _, _, err = client.GetRoles(context.Background(), KeysetPaginationVars{Limit: 50})
	if err == nil {
		t.Fatalf("expected an error on a 429 response")
	}

	st, ok := status.FromError(fmt.Errorf("baton-servicenow: failed to list roles: %w", err))
	if !ok {
		t.Fatalf("wrapped error did not resolve to a gRPC status")
	}

	var rl *v2.RateLimitDescription
	for _, detail := range st.Details() {
		if d, ok := detail.(*v2.RateLimitDescription); ok {
			rl = d
			break
		}
	}
	if rl == nil {
		t.Fatalf("no RateLimitDescription detail on a header-less 429")
	}

	// STATUS_OVERLIMIT is also what flags the failure as rate limiting in
	// baton-sdk pkg/metrics/instrumentor.go.
	if rl.GetStatus() != v2.RateLimitDescription_STATUS_OVERLIMIT {
		t.Errorf("status = %v, want STATUS_OVERLIMIT even with no headers", rl.GetStatus())
	}
	// remaining <= 0 and a future ResetAt are jointly what make the retryer
	// take the rate-limit branch instead of linear backoff.
	if rl.GetRemaining() > 0 {
		t.Errorf("remaining = %d, want <= 0 so the retryer waits until reset", rl.GetRemaining())
	}
	if wait := time.Until(rl.GetResetAt().AsTime()); wait < 45*time.Second || wait > 65*time.Second {
		t.Errorf("ResetAt is %v out, want the synthesized ~60s fallback", wait)
	}
}
