package servicenow

import (
	"context"
	"net/http"
	"testing"
)

func newTestRequest(t *testing.T) *http.Request {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "https://example.service-now.com/api/now/table/sys_user", nil)
	if err != nil {
		t.Fatalf("unexpected error building request: %v", err)
	}
	return req
}

func applyReqOpts(req *http.Request, opts ...ReqOpt) {
	for _, o := range opts {
		o(req)
	}
}

func TestBuildDomainQuery(t *testing.T) {
	tests := []struct {
		name    string
		field   string
		domains []string
		want    string
	}{
		{
			name:    "multiple domains are OR'd",
			field:   "user.email",
			domains: []string{"draftkings.com", "dk.com"},
			want:    "user.emailENDSWITH@draftkings.com^ORuser.emailENDSWITH@dk.com",
		},
		{
			name:    "single domain",
			field:   "email",
			domains: []string{"a.com"},
			want:    "emailENDSWITH@a.com",
		},
		{
			name:    "no domains is a no-op",
			field:   "email",
			domains: nil,
			want:    "",
		},
		{
			name:    "blank domains are skipped",
			field:   "email",
			domains: []string{"", "   "},
			want:    "",
		},
		{
			name:    "domains are trimmed and lowercased",
			field:   "email",
			domains: []string{" DraftKings.com "},
			want:    "emailENDSWITH@draftkings.com",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := buildDomainQuery(tc.field, tc.domains)
			if got != tc.want {
				t.Errorf("buildDomainQuery(%q, %v) = %q, want %q", tc.field, tc.domains, got, tc.want)
			}
		})
	}
}

// TestPrepareUserFilters_Regression pins the pre-refactor byte-for-byte
// output of prepareUserFilters now that it's built on top of buildDomainQuery.
func TestPrepareUserFilters_Regression(t *testing.T) {
	got := prepareUserFilters([]string{"a.com"}, nil)
	want := "emailENDSWITH@a.com"
	if got.Query != want {
		t.Errorf("prepareUserFilters(...).Query = %q, want %q", got.Query, want)
	}
}

func TestPrepareUserToRoleFilter(t *testing.T) {
	tests := []struct {
		name    string
		userId  string
		roleId  string
		domains []string
		want    string
	}{
		{
			name:    "enumeration (Grants) with allowed domains filters by domain",
			userId:  "",
			roleId:  "ROLE1",
			domains: []string{"draftkings.com"},
			want:    "role=ROLE1^user.emailENDSWITH@draftkings.com",
		},
		{
			name:    "enumeration with multiple allowed domains ORs the domain conditions, ANDed with role=",
			userId:  "",
			roleId:  "ROLE1",
			domains: []string{"draftkings.com", "dk.com"},
			want:    "role=ROLE1^user.emailENDSWITH@draftkings.com^ORuser.emailENDSWITH@dk.com",
		},
		{
			name:    "provisioning check for a specific user does not filter by domain",
			userId:  "USER1",
			roleId:  "ROLE1",
			domains: []string{"draftkings.com"},
			want:    "user=USER1^role=ROLE1",
		},
		{
			name:    "no allowed domains is a no-op",
			userId:  "",
			roleId:  "ROLE1",
			domains: nil,
			want:    "role=ROLE1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := prepareUserToRoleFilter(tc.userId, tc.roleId, tc.domains)
			if got.Query != tc.want {
				t.Errorf("prepareUserToRoleFilter(%q, %q, %v).Query = %q, want %q", tc.userId, tc.roleId, tc.domains, got.Query, tc.want)
			}
		})
	}
}

func TestPrepareUserToGroupFilter(t *testing.T) {
	tests := []struct {
		name    string
		userId  string
		groupId string
		domains []string
		want    string
	}{
		{
			name:    "enumeration (Grants) with allowed domains filters by domain",
			userId:  "",
			groupId: "GROUP1",
			domains: []string{"draftkings.com"},
			want:    "group=GROUP1^user.emailENDSWITH@draftkings.com",
		},
		{
			name:    "enumeration with multiple allowed domains ORs the domain conditions, ANDed with group=",
			userId:  "",
			groupId: "GROUP1",
			domains: []string{"draftkings.com", "dk.com"},
			want:    "group=GROUP1^user.emailENDSWITH@draftkings.com^ORuser.emailENDSWITH@dk.com",
		},
		{
			name:    "provisioning check for a specific user does not filter by domain",
			userId:  "USER1",
			groupId: "GROUP1",
			domains: []string{"draftkings.com"},
			want:    "user=USER1^group=GROUP1",
		},
		{
			name:    "no allowed domains is a no-op",
			userId:  "",
			groupId: "GROUP1",
			domains: nil,
			want:    "group=GROUP1",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := prepareUserToGroupFilter(tc.userId, tc.groupId, tc.domains)
			if got.Query != tc.want {
				t.Errorf("prepareUserToGroupFilter(%q, %q, %v).Query = %q, want %q", tc.userId, tc.groupId, tc.domains, got.Query, tc.want)
			}
		})
	}
}

func TestKeysetCursorFragment(t *testing.T) {
	tests := []struct {
		name   string
		lastID string
		want   string
	}{
		{
			name:   "first page orders by sys_id without seeking",
			lastID: "",
			want:   "ORDERBYsys_id",
		},
		{
			name:   "subsequent page seeks past lastID with the literal > operator",
			lastID: "abc123",
			want:   "sys_id>abc123^ORDERBYsys_id",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := keysetCursorFragment(tc.lastID)
			if got != tc.want {
				t.Errorf("keysetCursorFragment(%q) = %q, want %q", tc.lastID, got, tc.want)
			}
		})
	}
}

// TestKeysetPaginationVarsToReqOptions_ComposesWithFilterQuery mirrors how
// client.go builds requests: filter options first, then keyset pagination
// options appended after in the same ReqOpt slice. The seek condition must
// AND onto the existing filter query rather than clobbering it.
func TestKeysetPaginationVarsToReqOptions_ComposesWithFilterQuery(t *testing.T) {
	filterOpts := filterToReqOptions(prepareUserToRoleFilter("", "ROLE1", []string{"draftkings.com"}))
	paginationOpts := keysetPaginationVarsToReqOptions(&KeysetPaginationVars{Limit: 50, LastID: "abc123"})

	req := newTestRequest(t)
	applyReqOpts(req, filterOpts...)
	applyReqOpts(req, paginationOpts...)

	wantQuery := "role=ROLE1^user.emailENDSWITH@draftkings.com^sys_id>abc123^ORDERBYsys_id"
	if got := req.URL.Query().Get("sysparm_query"); got != wantQuery {
		t.Errorf("sysparm_query = %q, want %q", got, wantQuery)
	}
	if got := req.URL.Query().Get("sysparm_limit"); got != "50" {
		t.Errorf("sysparm_limit = %q, want %q", got, "50")
	}
	// ServiceNow computes X-Total-Count (a COUNT(*) over the whole filtered
	// set) unless told not to, which is expensive on every page. Termination
	// keys off an empty page only (see nextKeysetToken), so the count is
	// suppressed because it's never read.
	if got := req.URL.Query().Get("sysparm_no_count"); got != "true" {
		t.Errorf("sysparm_no_count = %q, want %q", got, "true")
	}
}

func TestWithNoCount(t *testing.T) {
	req := newTestRequest(t)
	applyReqOpts(req, WithNoCount())

	if got := req.URL.Query().Get("sysparm_no_count"); got != "true" {
		t.Errorf("sysparm_no_count = %q, want %q", got, "true")
	}
}

func TestNextKeysetToken(t *testing.T) {
	type item struct{ id string }
	idFn := func(i item) string { return i.id }

	// ServiceNow doesn't reliably honor sysparm_limit's exact row count, so
	// termination must key off an empty page only -- a short-but-nonempty
	// page must still continue, or the listing silently truncates.
	t.Run("short but nonempty page continues pagination", func(t *testing.T) {
		items := []item{{"a"}, {"b"}}
		got := nextKeysetToken(items, idFn)
		if got != "b" {
			t.Errorf("nextKeysetToken(...) = %q, want %q (must not treat a short page as the last one)", got, "b")
		}
	})

	t.Run("full page continues from the last row's sys_id", func(t *testing.T) {
		items := []item{{"a"}, {"b"}, {"c"}}
		got := nextKeysetToken(items, idFn)
		if got != "c" {
			t.Errorf("nextKeysetToken(...) = %q, want %q", got, "c")
		}
	})

	t.Run("empty page terminates pagination", func(t *testing.T) {
		got := nextKeysetToken([]item{}, idFn)
		if got != "" {
			t.Errorf("nextKeysetToken(...) = %q, want empty token", got)
		}
	})
}

// TestCappedForDomainFilter covers the page-size cap for the
// allowed-domains dot-walk filter (user.emailENDSWITH). The cap must apply
// only when the domain filter itself would apply (enumeration, userId=="",
// with allowed-domains configured) -- never to provisioning checks or when
// no domains are set.
func TestCappedForDomainFilter(t *testing.T) {
	tests := []struct {
		name    string
		userId  string
		domains []string
		limit   int
		want    int
	}{
		{
			name:    "enumeration with allowed domains caps a larger limit",
			userId:  "",
			domains: []string{"draftkings.com"},
			limit:   200,
			want:    domainFilteredPageSize,
		},
		{
			name:    "enumeration with allowed domains leaves an already-small limit alone",
			userId:  "",
			domains: []string{"draftkings.com"},
			limit:   10,
			want:    10,
		},
		{
			name:    "provisioning check (specific userId) is never capped",
			userId:  "USER1",
			domains: []string{"draftkings.com"},
			limit:   200,
			want:    200,
		},
		{
			name:    "no allowed domains is never capped",
			userId:  "",
			domains: nil,
			limit:   200,
			want:    200,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := cappedForDomainFilter(tc.userId, tc.domains, KeysetPaginationVars{Limit: tc.limit})
			if got.Limit != tc.want {
				t.Errorf("cappedForDomainFilter(%q, %v, Limit:%d).Limit = %d, want %d", tc.userId, tc.domains, tc.limit, got.Limit, tc.want)
			}
		})
	}
}
