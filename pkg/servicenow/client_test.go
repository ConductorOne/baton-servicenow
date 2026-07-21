package servicenow

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
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

	client, err := NewClient(server.Client(), "Basic dGVzdDp0ZXN0", "dev0", nil, nil, nil, server.URL)
	if err != nil {
		t.Fatalf("unexpected error creating client: %v", err)
	}

	page1, next1, err := client.GetRoles(context.Background(), KeysetPaginationVars{Limit: 50})
	if err != nil {
		t.Fatalf("unexpected error on page 1: %v", err)
	}
	if len(page1) != 50 {
		t.Errorf("page1 len = %d, want 50", len(page1))
	}
	if next1 == "" {
		t.Fatalf("expected a non-empty next token after a full page")
	}

	page2, next2, err := client.GetRoles(context.Background(), KeysetPaginationVars{Limit: 50, LastID: next1})
	if err != nil {
		t.Fatalf("unexpected error on page 2: %v", err)
	}
	if len(page2) != 3 {
		t.Errorf("page2 len = %d, want 3", len(page2))
	}
	if next2 == "" {
		t.Fatalf("pagination stopped after a short-but-nonempty page (3 rows) while more rows remained")
	}

	page3, next3, err := client.GetRoles(context.Background(), KeysetPaginationVars{Limit: 50, LastID: next2})
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

// TestGetRoles_IgnoresMalformedLegacyPaginationHeaders guards against a
// keyset page failing over the unrelated legacy Link-header/X-Total-Count
// computation, which keyset callers never read (see doRequestKeyset in
// client.go).
func TestGetRoles_IgnoresMalformedLegacyPaginationHeaders(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// doRequest's legacy pagination-token logic would fail parsing this
		// as an int; doRequestKeyset must never even attempt to.
		w.Header().Set("X-Total-Count", "not-a-number")
		w.Header().Set("Content-Type", "application/json")

		roles := []Role{{BaseResource: BaseResource{Id: "role-000"}}}
		if err := json.NewEncoder(w).Encode(ListResponse[Role]{Result: roles}); err != nil {
			t.Errorf("failed to encode test response: %v", err)
		}
	}))
	defer server.Close()

	client, err := NewClient(server.Client(), "Basic dGVzdDp0ZXN0", "dev0", nil, nil, nil, server.URL)
	if err != nil {
		t.Fatalf("unexpected error creating client: %v", err)
	}

	roles, _, err := client.GetRoles(context.Background(), KeysetPaginationVars{Limit: 50})
	if err != nil {
		t.Fatalf("unexpected error: %v (a malformed legacy pagination header must not fail a keyset page fetch)", err)
	}
	if len(roles) != 1 || roles[0].Id != "role-000" {
		t.Errorf("roles = %+v, want a single role-000", roles)
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

	client, err := NewClient(server.Client(), "Basic dGVzdDp0ZXN0", "dev0", nil, []string{"draftkings.com"}, nil, server.URL)
	if err != nil {
		t.Fatalf("unexpected error creating client: %v", err)
	}

	_, _, err = client.GetUsers(context.Background(), KeysetPaginationVars{Limit: 200})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := fmt.Sprintf("%d", domainFilteredPageSize)
	if gotLimit != want {
		t.Errorf("sysparm_limit sent = %q, want %q (AllowedDomains configured must cap the page size)", gotLimit, want)
	}
}
