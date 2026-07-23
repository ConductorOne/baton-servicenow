package servicenow

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

var (
	UserFields  = []string{"sys_id", "name", "roles", "user_name", "email", "first_name", "last_name", "active"}
	RoleFields  = []string{"sys_id", "grantable", "name"}
	GroupFields = []string{"sys_id", "description", "name"}
)

func queryMultipleIDs(ids []string) string {
	var preparedIDs []string

	for _, id := range ids {
		preparedIDs = append(preparedIDs, fmt.Sprintf("sys_id=%s", id))
	}

	return strings.Join(preparedIDs, "^OR")
}

var emptyOpt = func(_ *http.Request) {}

type ReqOpt func(req *http.Request)

func WithIncludeResponseBody() ReqOpt {
	return WithHeader("X-no-response-body", "false")
}

func WithHeader(key string, val string) ReqOpt {
	return func(req *http.Request) {
		req.Header.Set(key, val)
	}
}

func WithPageLimit(pageLimit int) ReqOpt {
	if pageLimit != 0 {
		return WithQueryParam("sysparm_limit", strconv.Itoa(pageLimit))
	}
	return emptyOpt
}

func WithOffset(offset int) ReqOpt {
	if offset != 0 {
		return WithQueryParam("sysparm_offset", strconv.Itoa(offset))
	}
	return emptyOpt
}

func WithQueryParam(key string, value string) ReqOpt {
	return func(req *http.Request) {
		q := req.URL.Query()
		q.Set(key, value)
		req.URL.RawQuery = q.Encode()
	}
}

func WithQuery(query string) ReqOpt {
	if query != "" {
		return WithQueryParam("sysparm_query", query)
	}
	return emptyOpt
}

// WithQueryAppend AND-appends extra onto whatever sysparm_query is already set on the request.
func WithQueryAppend(extra string) ReqOpt {
	if extra == "" {
		return emptyOpt
	}
	return func(req *http.Request) {
		merged := extra
		if existing := req.URL.Query().Get("sysparm_query"); existing != "" {
			merged = existing + "^" + extra
		}
		WithQueryParam("sysparm_query", merged)(req)
	}
}

func WithFields(fields ...string) ReqOpt {
	if len(fields) != 0 {
		return WithQueryParam("sysparm_fields", strings.Join(fields, ","))
	}
	return emptyOpt
}

func WithIncludeExternalRefLink() ReqOpt {
	return WithQueryParam("sysparm_exclude_reference_link", "false")
}

// WithNoCount skips ServiceNow's X-Total-Count computation (a COUNT(*) over
// the whole filtered set). Keyset termination doesn't need it -- it only
// checks for an empty page (see nextKeysetToken).
func WithNoCount() ReqOpt {
	return WithQueryParam("sysparm_no_count", "true")
}

type PaginationVars struct {
	Limit  int
	Offset int
}

// KeysetPaginationVars carries seek/keyset pagination state for Table API
// listings ordered by sys_id. Used by identity and membership endpoints
// (users, roles, groups, membership) instead of sysparm_offset, whose
// deep-offset requests degrade on large tables. Service Catalog/ticketing
// endpoints keep using PaginationVars/WithOffset.
type KeysetPaginationVars struct {
	Limit  int
	LastID string
}

// domainFilteredPageSize caps the page size for enumeration calls that add
// the allowed-domains dot-walk filter (user.emailENDSWITH@domain). ENDSWITH
// can't use the sys_id index, so a bigger page means more rows ServiceNow
// has to scan before it can respond.
const domainFilteredPageSize = 50

// cappedForDomainFilter caps vars.Limit to domainFilteredPageSize when the
// domain filter applies (userId=="" and allowed-domains configured);
// otherwise returns vars unchanged.
func cappedForDomainFilter(userId string, domains []string, vars KeysetPaginationVars) KeysetPaginationVars {
	if userId == "" && len(domains) > 0 && vars.Limit > domainFilteredPageSize {
		vars.Limit = domainFilteredPageSize
	}
	return vars
}

// keysetPaginationVarsToReqOptions sets sysparm_limit, appends the sys_id
// seek condition onto sysparm_query (must run after filterToReqOptions),
// and disables X-Total-Count via WithNoCount.
func keysetPaginationVarsToReqOptions(vars *KeysetPaginationVars) []ReqOpt {
	reqOpts := make([]ReqOpt, 0, 3)
	reqOpts = append(reqOpts, WithPageLimit(vars.Limit))
	reqOpts = append(reqOpts, WithQueryAppend(keysetCursorFragment(vars.LastID)))
	reqOpts = append(reqOpts, WithNoCount())
	return reqOpts
}

// buildKeysetReqOptions composes a filter with keyset pagination in the
// only valid order: the seek condition must be appended after the filter
// sets sysparm_query.
func buildKeysetReqOptions(filterVars *FilterVars, keysetVars *KeysetPaginationVars) []ReqOpt {
	reqOpts := filterToReqOptions(filterVars)
	return append(reqOpts, keysetPaginationVarsToReqOptions(keysetVars)...)
}

// keysetCursorFragment builds the sysparm_query fragment that seeks past
// lastID. ORDERBYsys_id is required on every page, including the first,
// so the cursor stays consistent with how the page is ordered.
func keysetCursorFragment(lastID string) string {
	if lastID != "" {
		return fmt.Sprintf("sys_id>%s^ORDERBYsys_id", lastID)
	}
	return "ORDERBYsys_id"
}

// nextKeysetToken derives the next seek token from a keyset page.
// Termination is decided solely by an empty page, never by len(items) <
// limit: ServiceNow doesn't always return exactly the requested row count,
// so a short-but-nonempty page must not be treated as the last one.
func nextKeysetToken[T any](items []T, idFn func(T) string) string {
	if len(items) == 0 {
		return ""
	}
	return idFn(items[len(items)-1])
}

type FilterVars struct {
	Fields []string
	Query  string
	UserId string
}

// buildDomainQuery builds an OR'd ENDSWITH condition over emailField for
// each domain (e.g. "emailENDSWITH@a.com^ORemailENDSWITH@b.com"). Returns
// "" when domains is empty.
func buildDomainQuery(emailField string, domains []string) string {
	var queries []string

	for _, domain := range domains {
		d := strings.TrimSpace(strings.ToLower(domain))
		if d != "" {
			queries = append(queries, fmt.Sprintf("%sENDSWITH@%s", emailField, d))
		}
	}

	return strings.Join(queries, "^OR")
}

func prepareUserFilters(domains []string, customFields []string) *FilterVars {
	fields := UserFields
	for _, f := range customFields {
		if strings.HasPrefix(f, "u_") {
			fields = append(fields, f)
		}
	}

	return &FilterVars{
		Fields: fields,
		Query:  buildDomainQuery("email", domains),
	}
}

func prepareRoleFilters() *FilterVars {
	return &FilterVars{
		Fields: RoleFields,
		Query:  "grantable=true",
	}
}

func prepareGroupFilters(ids []string) *FilterVars {
	var query string

	if ids != nil {
		query = queryMultipleIDs(ids)
	}

	return &FilterVars{
		Fields: GroupFields,
		Query:  query,
	}
}

// prepareUserToGroupFilter builds the sys_user_grmember filter. When userId
// is empty (enumerating all members, not checking one user for
// provisioning), it also scopes user.email to the allowed domains, so
// group grants stay consistent with which users actually get synced.
func prepareUserToGroupFilter(userId string, groupId string, domains []string) *FilterVars {
	var conditions []string

	if userId != "" {
		conditions = append(conditions, fmt.Sprintf("user=%s", userId))
	}

	if groupId != "" {
		conditions = append(conditions, fmt.Sprintf("group=%s", groupId))
	}

	if userId == "" {
		if domainQuery := buildDomainQuery("user.email", domains); domainQuery != "" {
			conditions = append(conditions, domainQuery)
		}
	}

	return &FilterVars{
		Fields: []string{
			"sys_id", "user", "group",
		},
		Query: strings.Join(conditions, "^"),
	}
}

// prepareUserToRoleFilter builds the sys_user_has_role filter. See
// prepareUserToGroupFilter for why the domain filter is gated on userId=="".
func prepareUserToRoleFilter(userId string, roleId string, domains []string) *FilterVars {
	var conditions []string

	if userId != "" {
		conditions = append(conditions, fmt.Sprintf("user=%s", userId))
	}

	if roleId != "" {
		conditions = append(conditions, fmt.Sprintf("role=%s", roleId))
	}

	if userId == "" {
		if domainQuery := buildDomainQuery("user.email", domains); domainQuery != "" {
			conditions = append(conditions, domainQuery)
		}
	}

	return &FilterVars{
		Fields: []string{
			"sys_id", "user", "role", "inherited",
		},
		Query: strings.Join(conditions, "^"),
	}
}

func prepareGroupToRoleFilter(groupId string, roleId string) *FilterVars {
	var query string
	if groupId != "" {
		query = fmt.Sprintf("group=%s", groupId)
	}

	if roleId != "" {
		if query != "" {
			query = fmt.Sprintf("%s^role=%s", query, roleId)
		} else {
			query = fmt.Sprintf("role=%s", roleId)
		}
	}

	return &FilterVars{
		Fields: []string{
			"sys_id", "role", "group", "inherits",
		},
		Query: query,
	}
}

func filterToReqOptions(vars *FilterVars) []ReqOpt {
	reqOpts := make([]ReqOpt, 0)
	reqOpts = append(reqOpts, WithQuery(vars.Query))
	if len(vars.Fields) != 0 {
		reqOpts = append(reqOpts, WithFields(vars.Fields...))
	}
	return reqOpts
}

func paginationVarsToReqOptions(vars *PaginationVars) []ReqOpt {
	reqOpts := make([]ReqOpt, 0)
	reqOpts = append(reqOpts, WithPageLimit(vars.Limit))
	reqOpts = append(reqOpts, WithOffset(vars.Offset))
	return reqOpts
}
