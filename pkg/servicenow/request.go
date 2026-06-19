package servicenow

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

var (
	UserFields  = []string{"sys_id", "name", "roles", "user_name", "email", "first_name", "last_name", "active", "sys_updated_on"}
	RoleFields  = []string{"sys_id", "grantable", "name", "sys_updated_on"}
	GroupFields = []string{"sys_id", "description", "name", "manager", "sys_updated_on"}
)

// UpdatedSinceField is the ServiceNow column used to filter rows by last-change
// time. Every Table API row carries it; comparisons in sysparm_query are
// evaluated in the API caller's session timezone (UTC for typical integration
// accounts), and the value is itself a UTC "YYYY-MM-DD HH:MM:SS".
const UpdatedSinceField = "sys_updated_on"

// WithUpdatedSince appends a "sys_updated_on>=<ts>" clause to an existing
// FilterVars query, returning a watermark-filtered query string. ts must be
// a ServiceNow datetime literal ("YYYY-MM-DD HH:MM:SS"). An empty ts is a
// no-op (full pull). The clause is ANDed (^) with any existing query.
func appendUpdatedSince(query string, ts string) string {
	if ts == "" {
		return query
	}
	clause := fmt.Sprintf("%s>=%s", UpdatedSinceField, ts)
	if query == "" {
		return clause
	}
	return query + "^" + clause
}

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

func WithFields(fields ...string) ReqOpt {
	if len(fields) != 0 {
		return WithQueryParam("sysparm_fields", strings.Join(fields, ","))
	}
	return emptyOpt
}

func WithIncludeExternalRefLink() ReqOpt {
	return WithQueryParam("sysparm_exclude_reference_link", "false")
}

type PaginationVars struct {
	Limit  int
	Offset int
}

type FilterVars struct {
	Fields []string
	Query  string
	UserId string
}

func prepareUserFilters(domains []string, customFields []string) *FilterVars {
	var queries []string

	for _, domain := range domains {
		d := strings.TrimSpace(strings.ToLower(domain))
		if d != "" {
			queries = append(queries, fmt.Sprintf("emailENDSWITH@%s", d))
		}
	}

	combined := strings.Join(queries, "^OR")

	fields := UserFields
	for _, f := range customFields {
		if strings.HasPrefix(f, "u_") {
			fields = append(fields, f)
		}
	}

	return &FilterVars{
		Fields: fields,
		Query:  combined,
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

func prepareUserToGroupFilter(userId string, groupId string) *FilterVars {
	var query string

	if userId != "" {
		query = fmt.Sprintf("user=%s", userId)
	}

	if groupId != "" {
		if query != "" {
			query = fmt.Sprintf("%s^group=%s", query, groupId)
		} else {
			query = fmt.Sprintf("group=%s", groupId)
		}
	}

	return &FilterVars{
		Fields: []string{
			"sys_id", "user", "group", "sys_updated_on",
		},
		Query: query,
	}
}

func prepareUserToRoleFilter(userId string, roleId string) *FilterVars {
	var query string
	if userId != "" {
		query = fmt.Sprintf("user=%s", userId)
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
			"sys_id", "user", "role", "inherited", "sys_updated_on",
		},
		Query: query,
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
			"sys_id", "role", "group", "inherits", "sys_updated_on",
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
