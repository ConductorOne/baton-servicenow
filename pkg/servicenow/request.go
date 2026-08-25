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
// human-chosen ids like "glean_user_role", arbitrary punctuation like
// "qa:colon:example", or hex with a stray trailing character. This is a
// disallow-list, not an allow-list: it excludes only the characters
// (^, =, >, <, whitespace, quotes) that carry meaning in ServiceNow's query
// language and would let a cursor value escape the sysparm_query fragment
// it's interpolated into unescaped (see keysetCursorFragment) -- this regex
// is a query-injection guard, not just a format check. Everything else a
// sys_id can legally contain, including ':', is accepted.
//
// Excluding ^ does double duty: ServiceNow's sysparm_query AND operator has
// no escape mechanism (a URL-encoded %5E is decoded server-side and still
// parsed as AND), so a cursor containing it could never be seeked past
// anyway -- see nextKeysetPageToken's offset fallback for that case. That
// same guarantee (no valid cursor ever contains ^) is also what makes ^ safe
// to reuse below as the token's own cursor/offset delimiter.
const cursorCharset = `[^\^=<>'"\s]{1,32}`

// Matches "cursor" or "cursor^offset". ^ can't appear inside cursorCharset
// (see its comment), so it unambiguously marks the offset delimiter even
// though the cursor itself may now contain ':' or other punctuation. The
// cursor is optional: the first window of a listing can be the emptied one,
// leaving nothing to seek from.
var keysetTokenPattern = regexp.MustCompile(`^(` + cursorCharset + `)?(?:\^([0-9]{1,18}))?$`)

var cursorPattern = regexp.MustCompile(`^` + cursorCharset + `$`)

// allDigitsPattern matches a purely numeric string. Go's RE2 engine has no
// lookahead, so "at least one non-digit character" is checked as a second,
// separate pattern rather than folded into cursorPattern.
var allDigitsPattern = regexp.MustCompile(`^[0-9]+$`)

// legacyOffsetSuffixPattern matches a bare cursor ending in ":<digits>" --
// the exact shape v1.1.19 through v1.1.22 (all released, though never to the
// stable channel) encoded for "cursor with a skip offset" using ':' as the
// delimiter. Now that ':' is a legal cursor character in its own right, that
// shape is genuinely ambiguous (a real cursor can end in ":<digits>" too),
// so ParseKeysetToken rejects it outright rather than guessing -- silently
// treating "glean_user_role:50" as a bare cursor with the ":50" dropped
// would seek from a cursor that never existed and skip every row the
// original offset was meant to step past, succeeding while quietly losing
// data. Rejecting it converts that into the same loud, safe failure already
// used for a stale pre-keyset token (looksLikeStaleOffsetToken).
var legacyOffsetSuffixPattern = regexp.MustCompile(`:[0-9]{1,18}$`)

// looksLikeStaleOffsetToken reports whether a bare cursor string (no
// ":offset" segment) is indistinguishable from a stale token left over from
// baton-servicenow's pre-keyset (<=v1.1.18) offset-based pagination, which
// encoded the page position as a short bare integer (e.g. "150"). A canonical
// sys_id is always exactly 32 characters and can legitimately be all-digits
// (nothing requires a hex GUID to contain a letter), so length -- not the
// digits themselves -- is what distinguishes the two: a page offset never
// reaches 32 digits.
//
// This only matters for a BARE cursor. EncodeKeysetToken forces the
// "^offset" form (even for offset 0) whenever lastID itself would collide
// with this shape, or with legacyOffsetSuffixPattern's, which removes the
// ambiguity at the source instead of asking the parser to guess.
func looksLikeStaleOffsetToken(cursor string) bool {
	return len(cursor) != 32 && allDigitsPattern.MatchString(cursor)
}

// EncodeKeysetToken is the only producer of the page token format. A bare cursor
// is the common shape: nothing to skip. Validates lastID's charset -- the
// query-injection guard -- so a sys_id with disallowed characters fails here,
// at the page that produced it, with the row's own context in the error --
// not one page later, parsed out of a token with none.
func EncodeKeysetToken(lastID string, offset int) (string, error) {
	if lastID != "" && !cursorPattern.MatchString(lastID) {
		return "", fmt.Errorf("cannot build page cursor: sys_id %q is outside the allowed cursor charset %s", lastID, cursorPattern.String())
	}
	if hasJavascriptSchemePrefix(lastID) {
		return "", fmt.Errorf("cannot build page cursor: sys_id %q begins with a javascript: scheme, which ServiceNow evaluates server-side when it lands after sys_id>", lastID)
	}
	// A short all-numeric sys_id, or one ending in ":<digits>", encoded
	// bare, is indistinguishable from a stale pre-keyset token or a
	// v1.1.19-v1.1.22 ':'-delimited cursor/offset pair (see
	// looksLikeStaleOffsetToken, legacyOffsetSuffixPattern, and
	// ParseKeysetToken). Force the "^offset" form even at offset 0 so the
	// parser can tell them apart from a real bare cursor by shape alone.
	if offset == 0 && !looksLikeStaleOffsetToken(lastID) && !legacyOffsetSuffixPattern.MatchString(lastID) {
		return lastID, nil
	}
	return fmt.Sprintf("%s^%d", lastID, offset), nil
}

// hasJavascriptSchemePrefix reports whether cursor begins with a
// "javascript:" scheme (case-insensitive), ServiceNow's encoded-query
// idiom for evaluating a script server-side (e.g.
// "sys_created_on>javascript:gs.beginningOfToday()"). keysetCursorFragment
// interpolates the cursor directly after "sys_id>", the exact prefix
// position that idiom needs, so a cursor value can't be allowed to start
// with it even though ':' is otherwise a legal cursor character.
func hasJavascriptSchemePrefix(cursor string) bool {
	const scheme = "javascript:"
	return len(cursor) >= len(scheme) && strings.EqualFold(cursor[:len(scheme)], scheme)
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

	hasOffsetSegment := match[2] != ""
	// A bare (no "^offset" segment) purely numeric cursor is a stale
	// pre-keyset token, not a sys_id -- reject it rather than mis-seeking on
	// it. A short all-numeric sys_id is never encoded bare (see
	// EncodeKeysetToken), so this can't reject a real cursor.
	if match[1] != "" && !hasOffsetSegment && looksLikeStaleOffsetToken(match[1]) {
		return "", 0, fmt.Errorf("malformed page token %q: %q is not a valid cursor", token, match[1])
	}
	// A bare cursor ending in ":<digits>" is the v1.1.19-v1.1.22
	// ':'-delimited "cursor:offset" shape (see legacyOffsetSuffixPattern).
	// Reject it rather than silently treating it as a bare cursor with the
	// offset dropped.
	if match[1] != "" && !hasOffsetSegment && legacyOffsetSuffixPattern.MatchString(match[1]) {
		return "", 0, fmt.Errorf("malformed page token %q: %q looks like a legacy ':'-delimited cursor/offset pair", token, match[1])
	}
	// Defense in depth: EncodeKeysetToken already refuses to produce a
	// javascript:-prefixed cursor, but the parser re-validates the charset
	// independently of its producer, so it re-validates this too rather than
	// trusting every token handed to it came from EncodeKeysetToken. Applies
	// regardless of hasOffsetSegment -- the prefix is dangerous in either shape.
	if hasJavascriptSchemePrefix(match[1]) {
		return "", 0, fmt.Errorf("malformed page token %q: cursor %q begins with a javascript: scheme, which ServiceNow evaluates server-side when it lands after sys_id>", token, match[1])
	}

	var offset int
	if hasOffsetSegment {
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
