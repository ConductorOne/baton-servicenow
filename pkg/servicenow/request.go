package servicenow

import (
	"fmt"
	"net/http"
	"regexp"
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
	Limit  int    // page size, and the most rows a call may return
	LastID string // seek cursor
	// Offset is non-zero only while stepping past an ACL-emptied window, where
	// the cursor cannot advance on its own.
	Offset int
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
// seek condition onto sysparm_query (must run after filterToReqOptions), and
// carries the skip offset. X-Total-Count is left on: nextSkipToken needs it to
// tell an ACL-emptied window from the end of the table.
func keysetPaginationVarsToReqOptions(vars *KeysetPaginationVars) []ReqOpt {
	reqOpts := make([]ReqOpt, 0, 3)
	reqOpts = append(reqOpts, WithPageLimit(vars.Limit))
	reqOpts = append(reqOpts, WithQueryAppend(keysetCursorFragment(vars.LastID)))
	reqOpts = append(reqOpts, WithOffset(vars.Offset))
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

// nextKeysetToken derives the seek cursor from a page that returned rows. Never
// keys off len(items) < limit: ServiceNow doesn't always return exactly the
// requested count, so a short-but-nonempty page must not end the listing. What an
// empty page means is nextSkipToken's call, not this one's.
func nextKeysetToken[T any](items []T, idFn func(T) string) (string, error) {
	if len(items) == 0 {
		return "", nil
	}
	return EncodeKeysetToken(idFn(items[len(items)-1]), 0)
}

// cursorPattern is the charset EncodeKeysetToken accepts for a sys_id-derived
// cursor. ServiceNow sys_ids aren't always canonical 32-char hex GUIDs: rows
// written by third-party update sets (seen in practice on sys_user_role,
// sys_user_has_role, sys_group_has_role) can carry short, mixed-case,
// human-chosen ids like "glean_user_role", or hex with a stray trailing
// character. This is widened to admit that real-world variance while still
// excluding characters (^, =, >, whitespace, quotes, ...) that would let a
// cursor value escape the sysparm_query fragment it's interpolated into
// unescaped (see keysetCursorFragment) -- this regex is a query-injection
// guard, not just a format check.
const cursorCharset = `[0-9A-Za-z_.\-]{1,32}`

// Matches "cursor" or "cursor:offset". The cursor is optional: the first window
// of a listing can be the emptied one, leaving nothing to seek from.
var keysetTokenPattern = regexp.MustCompile(`^(` + cursorCharset + `)?(?::([0-9]{1,18}))?$`)

var cursorPattern = regexp.MustCompile(`^` + cursorCharset + `$`)

// allDigitsPattern matches a purely numeric string. Go's RE2 engine has no
// lookahead, so "at least one non-digit character" is checked as a second,
// separate pattern rather than folded into cursorPattern.
var allDigitsPattern = regexp.MustCompile(`^[0-9]+$`)

// isValidCursor reports whether id is an acceptable keyset cursor: within the
// allowed charset, and -- if purely numeric -- 32 characters long. That
// length check, not the digits themselves, is what tells a real cursor apart
// from a stale token left over from baton-servicenow's pre-keyset (<=v1.1.18)
// offset-based pagination, which encoded the page position as a short bare
// integer (e.g. "150"): a canonical sys_id is always exactly 32 characters
// and can legitimately be all-digits (nothing requires a hex GUID to contain
// a letter), while a page offset never reaches 32 digits. Rejecting only
// short numeric strings keeps a resumed pre-keyset sync failing loudly
// instead of being silently misread as a keyset cursor.
func isValidCursor(id string) bool {
	if !cursorPattern.MatchString(id) {
		return false
	}
	return len(id) == 32 || !allDigitsPattern.MatchString(id)
}

// EncodeKeysetToken is the only producer of the page token format. A bare cursor
// is the common shape: nothing to skip. Validates lastID so a non-conforming
// sys_id fails here, at the page that produced it, with the row's own context
// in the error -- not one page later, parsed out of a token with none.
func EncodeKeysetToken(lastID string, offset int) (string, error) {
	if lastID != "" && !isValidCursor(lastID) {
		return "", fmt.Errorf("cannot build page cursor: sys_id %q is not a valid cursor (must match %s and not be purely numeric)", lastID, cursorPattern.String())
	}
	if offset == 0 {
		return lastID, nil
	}
	return fmt.Sprintf("%s:%d", lastID, offset), nil
}

// ParseKeysetToken is the only consumer of it, returning the cursor and the skip
// offset. The cursor is returned as-is: case is significant in a ServiceNow
// sys_id and must not be normalized away (an earlier version lower-cased every
// cursor, corrupting mixed-case ids on the very next page request).
func ParseKeysetToken(token string) (string, int, error) {
	if token == "" {
		return "", 0, nil
	}

	match := keysetTokenPattern.FindStringSubmatch(token)
	if match == nil {
		return "", 0, fmt.Errorf("malformed page token %q", token)
	}
	// A purely numeric cursor is a stale pre-keyset (<=v1.1.18) offset token,
	// not a sys_id -- reject it here rather than mis-seeking on it (see
	// isValidCursor).
	if match[1] != "" && !isValidCursor(match[1]) {
		return "", 0, fmt.Errorf("malformed page token %q: %q is not a valid cursor", token, match[1])
	}

	var offset int
	if match[2] != "" {
		parsed, err := strconv.Atoi(match[2])
		if err != nil {
			return "", 0, fmt.Errorf("malformed offset in page token %q: %w", token, err)
		}
		offset = parsed
	}

	return match[1], offset, nil
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
