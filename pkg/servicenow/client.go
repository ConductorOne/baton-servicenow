package servicenow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/tomnomnom/linkheader"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	BaseURL          = "https://%s.service-now.com/api"
	TableAPIBaseURL  = BaseURL + "/now/table"
	GlobalApiBaseURL = BaseURL + "/global"

	UsersBaseUrl = TableAPIBaseURL + "/sys_user"
	UserBaseUrl  = UsersBaseUrl + "/%s"

	GroupsBaseUrl = TableAPIBaseURL + "/sys_user_group"
	GroupBaseUrl  = GroupsBaseUrl + "/%s"

	GroupMembersBaseUrl      = TableAPIBaseURL + "/sys_user_grmember"
	GroupMemberDetailBaseUrl = GroupMembersBaseUrl + "/%s"

	RolesBaseUrl           = TableAPIBaseURL + "/sys_user_role"
	UserRolesBaseUrl       = TableAPIBaseURL + "/sys_user_has_role"
	UserRoleDetailBaseUrl  = UserRolesBaseUrl + "/%s"
	GroupRolesBaseUrl      = TableAPIBaseURL + "/sys_group_has_role"
	GroupRoleDetailBaseUrl = GroupRolesBaseUrl + "/%s"

	UserRoleInheritanceBaseUrl = GlobalApiBaseURL + "/user_role_inheritance"

	// On-Call Scheduling.
	RotasBaseUrl            = TableAPIBaseURL + "/cmn_rota"
	RotaDetailBaseUrl       = RotasBaseUrl + "/%s"
	RostersBaseUrl          = TableAPIBaseURL + "/cmn_rota_roster"
	RosterDetailBaseUrl     = RostersBaseUrl + "/%s"
	RotaMembersBaseUrl      = TableAPIBaseURL + "/cmn_rota_member"
	RotaMemberDetailBaseUrl = RotaMembersBaseUrl + "/%s"

	// On-Call member provisioning action tables (engine-processed server-side).
	OnCallAddMemberUrl    = TableAPIBaseURL + "/on_call_add_member"
	OnCallRemoveMemberUrl = TableAPIBaseURL + "/on_call_remove_member"

	// On-Call REST API (not the Table API): returns who is on call now.
	WhoIsOnCallUrl = BaseURL + "/now/on_call_rota/whoisoncall"

	// Service Catalogs.
	ServiceCatalogRequestedItemBaseUrl        = TableAPIBaseURL + "/sc_req_item"
	ServiceCatalogRequestedItemDetailsBaseUrl = ServiceCatalogRequestedItemBaseUrl + "/%s"

	ServiceCatalogRequestBaseUrl        = TableAPIBaseURL + "/sc_request"
	ServiceCatalogRequestDetailsBaseUrl = ServiceCatalogRequestBaseUrl + "/%s"

	ServiceCatalogBaseUrl         = BaseURL + "/sn_sc/servicecatalog"
	ServiceCatalogListCatalogsUrl = ServiceCatalogBaseUrl + "/catalogs"
	ServiceCatalogGetCatalogUrl   = ServiceCatalogListCatalogsUrl + "/%s"

	ServiceCatalogCategoryBaseUrl       = ServiceCatalogGetCatalogUrl + "/categories"
	ServiceCatalogCategoryDetailBaseUrl = ServiceCatalogCategoryBaseUrl + "/%s"

	ServiceCatalogItemBaseUrl      = ServiceCatalogBaseUrl + "/items"
	ServiceCatalogItemGetUrl       = ServiceCatalogItemBaseUrl + "/%s"
	ServiceCatalogItemVariablesUrl = ServiceCatalogItemGetUrl + "/variables"

	ServiceCatalogOrderItemUrl = ServiceCatalogItemGetUrl + "/order_now"

	LabelBaseUrl      = TableAPIBaseURL + "/label"
	LabelEntryBaseUrl = TableAPIBaseURL + "/label_entry"

	// To get possible states for service catalog requested item.
	ChoiceBaseUrl = TableAPIBaseURL + "/sys_choice"

	InstanceURLTemplate = `{{.Deployment}}.service-now.com`

	// Variable sets & variables (Table API).
	VariableSetM2MBaseUrl = TableAPIBaseURL + "/io_set_item"
	ItemOptionNewBaseUrl  = TableAPIBaseURL + "/item_option_new" // variables (questions)
	QuestionChoiceBaseUrl = TableAPIBaseURL + "/question_choice" // option lists
)

type ListResponse[T any] struct {
	Result []T `json:"result"`
}

type SingleResponse[T any] struct {
	Result T `json:"result"`
}

type IDResponse = SingleResponse[BaseResource]
type UserResponse = SingleResponse[User]
type GroupResponse = SingleResponse[Group]
type RotaResponse = SingleResponse[Rota]
type RosterResponse = SingleResponse[Roster]
type WhoIsOnCallResponse = ListResponse[OnCallMember]
type CatalogsResponse = ListResponse[Catalog]
type CatalogItemsResponse = ListResponse[CatalogItem]
type CatalogItemResponse = SingleResponse[CatalogItem]
type CatalogItemVariablesResponse = ListResponse[CatalogItemVariable]
type OrderCatalogItemResponse = SingleResponse[RequestInfo]
type RequestItemResponse = SingleResponse[RequestedItem]
type RequestItemsResponse = ListResponse[RequestedItem]
type ServiceCatalogRequestResponse = SingleResponse[ServiceCatalogRequest]
type LabelResponse = SingleResponse[Label]
type LabelsResponse = ListResponse[Label]
type LabelEntriesLabelNameResponse = ListResponse[LabelEntryName]
type RequestedItemStateResponse = ListResponse[RequestItemState]
type VariableSetM2MResponse = ListResponse[VariableSetM2M]
type VariableSetsResponse = ListResponse[VariableSet]
type ItemOptionNewResponse = ListResponse[ItemOptionNew]
type QuestionChoiceResponse = ListResponse[QuestionChoice]

type Client struct {
	httpClient          *uhttp.BaseHttpClient
	auth                string
	deployment          string
	baseURL             string
	baseURLOverride     bool
	TicketSchemaFilters map[string]string
	AllowedDomains      []string
	CustomUserFields    []string
}

// Official documentation.
// https://developer.servicenow.com/dev.do#!/reference/api/rome/rest/c_TableAPI .
// https://www.servicenow.com/docs/bundle/yokohama-api-reference/page/integrate/inbound-rest/concept/c_TableAPI.html .
// https://developer.servicenow.com/dev.do#!/reference/api/yokohama/rest/c_TableAPI?navFilter=table .

func NewClient(
	httpClient *uhttp.BaseHttpClient,
	auth string,
	deployment string,
	ticketSchemaFilters map[string]string,
	allowedDomains []string,
	customUserFields []string,
	baseURLOverride string,
) (*Client, error) {
	var baseURL string
	if baseURLOverride != "" {
		// apiURL splices the override in as a URL prefix, so it has to be an
		// absolute URL. url.Parse on its own accepts nearly any string, so the
		// scheme/host check is what actually catches a misconfigured flag.
		parsed, err := url.Parse(baseURLOverride)
		if err != nil {
			return nil, fmt.Errorf("invalid base URL %q: %w", baseURLOverride, err)
		}
		if parsed.Scheme == "" || parsed.Host == "" {
			return nil, fmt.Errorf("invalid base URL %q: must include a scheme and host, e.g. https://example.service-now.com", baseURLOverride)
		}
		baseURL = baseURLOverride
	} else {
		var err error
		baseURL, err = GenerateURL(InstanceURLTemplate, map[string]string{"Deployment": deployment})
		if err != nil {
			return nil, err
		}
		// Unlike the override, this one is a bare host (see
		// InstanceURLTemplate) -- ticket.go composes it as
		// url.URL{Scheme: "https", Host: baseURL}. Validate it as a host so a
		// malformed deployment fails at startup instead of yielding a broken
		// request URL mid-sync.
		if parsed, err := url.Parse("https://" + baseURL); err != nil || parsed.Host != baseURL {
			return nil, fmt.Errorf("invalid deployment %q: produced unusable instance host %q", deployment, baseURL)
		}
	}
	return &Client{
		httpClient:          httpClient,
		auth:                auth,
		deployment:          deployment,
		baseURL:             baseURL,
		baseURLOverride:     baseURLOverride != "",
		TicketSchemaFilters: ticketSchemaFilters,
		AllowedDomains:      allowedDomains,
		CustomUserFields:    customUserFields,
	}, nil
}

func (c *Client) GetBaseURL() string {
	return c.baseURL
}

// apiURL builds an API URL from a constant pattern like UsersBaseUrl.
// When a base URL override is set, it replaces the default
// https://DEPLOYMENT.service-now.com/api prefix with the override.
func (c *Client) apiURL(pattern string, args ...any) string {
	expanded := fmt.Sprintf(pattern, args...)
	if c.baseURLOverride {
		defaultBase := fmt.Sprintf("https://%s.service-now.com/api", c.deployment)
		return strings.Replace(expanded, defaultBase, c.baseURL, 1)
	}
	return expanded
}

// getKeysetPage runs one keyset-paginated Table API GET. Uses c.getKeyset, not
// c.get: keyset callers derive their own token and don't need doRequest's legacy
// Link-header one. What follows the page is nextKeysetPageToken's call.
func getKeysetPage[T any](
	ctx context.Context,
	c *Client,
	url string,
	filterVars *FilterVars,
	keysetVars *KeysetPaginationVars,
	idFn func(T) string,
) ([]T, string, annotations.Annotations, error) {
	if keysetVars.Limit <= 0 {
		return nil, "", nil, fmt.Errorf("keyset pagination called with Limit %d for %s", keysetVars.Limit, url)
	}

	var resp ListResponse[T]
	header, annos, err := c.getKeyset(ctx, url, &resp, buildKeysetReqOptions(filterVars, keysetVars)...)
	if err != nil {
		return nil, "", annos, err
	}

	token, err := nextKeysetPageToken(ctx, url, header, keysetVars, resp.Result, idFn)
	if err != nil {
		return nil, "", annos, fmt.Errorf("%s: %w", url, err)
	}
	// An empty token ends the listing, so a page that returned rows must never
	// produce one -- that would drop the rest of the table without a trace.
	// Except a point lookup (Limit<=1): it never continues pagination, so an
	// empty token alongside its one matched row is the intended outcome, not
	// a dropped table.
	if keysetVars.Limit > 1 && len(resp.Result) > 0 && token == "" {
		return nil, "", annos, fmt.Errorf("page returned %d rows but no cursor for %s", len(resp.Result), url)
	}
	return resp.Result, token, annos, nil
}

// nextKeysetPageToken decides what follows this page: advance the cursor, step
// past a window row-level ACLs emptied or whose last row can't be seeked on,
// or end the listing.
func nextKeysetPageToken[T any](
	ctx context.Context,
	url string,
	header http.Header,
	v *KeysetPaginationVars,
	items []T,
	idFn func(T) string,
) (string, error) {
	// A point lookup (Limit<=1, e.g. a Grant/Revoke idempotency check) never
	// continues pagination -- its caller always discards the token -- so don't
	// compute one. Otherwise a found row whose sys_id fails cursor validation
	// would turn "the grant already exists" into a hard error instead of the
	// GrantAlreadyExists/GrantAlreadyRevoked annotation callers rely on.
	if v.Limit <= 1 {
		return "", nil
	}

	if len(items) == 0 {
		return nextSkipToken(ctx, url, header, v)
	}

	if v.Offset > 0 {
		ctxzap.Extract(ctx).Debug("baton-servicenow: stepped past rows the sync account cannot read",
			zap.String("url", url),
			zap.String("cursor", v.LastID),
			zap.Int("offset", v.Offset),
		)
	}
	// Readable rows: seek from the last one, and any skip offset is spent.
	token, err := nextKeysetToken(items, idFn)
	if err == nil {
		return token, nil
	}

	// The last row's sys_id can't be used as a seek value at all -- most
	// notably one containing ^, ServiceNow's AND operator, which has no
	// escape mechanism in sysparm_query (a URL-encoded %5E is decoded back
	// to ^ server-side and parsed as the operator). Falling through to that
	// error would exit the sync and, because the row persists in the source
	// table, fail identically on every subsequent run. Instead, step past
	// this page by offset from the same base cursor.
	//
	// This steps by len(items), not v.Limit like nextSkipToken's ACL-emptied
	// fallback does: sysparm_limit applies before row-level ACLs, so a
	// thinned page can return far fewer rows than Limit, and stepping by
	// Limit would skip over unread rows the next request should still see.
	// The tradeoff is that on a heavily ACL-thinned page this fallback can
	// fire again on the next hop, re-emitting the unseekable row each time --
	// it still terminates, just not in one step.
	next := v.Offset + len(items)
	fallbackToken, fallbackErr := EncodeKeysetToken(v.LastID, next)
	if fallbackErr != nil {
		return "", err
	}
	ctxzap.Extract(ctx).Debug("baton-servicenow: last row's sys_id cannot be used as a seek cursor, stepping past this window by offset instead",
		zap.String("url", url),
		zap.String("cursor", v.LastID),
		zap.Int("offset", next),
		zap.Error(err),
	)
	return fallbackToken, nil
}

// nextSkipToken decides what an empty page means: the end of the listing (""),
// or a window row-level ACLs emptied (a token stepping the offset past it).
// X-Total-Count counts matching rows before ACLs run, so it still sees the rows
// this page could not.
// nextKeysetPageToken already returns before calling this for Limit<=1, so an
// empty page here always means a real window, never a point lookup.
func nextSkipToken(ctx context.Context, url string, header http.Header, v *KeysetPaginationVars) (string, error) {
	l := ctxzap.Extract(ctx)

	count, ok := readTotalCount(header)
	if !ok {
		l.Debug("baton-servicenow: ending the listing, row count unavailable",
			zap.String("url", url),
			zap.String("cursor", v.LastID),
		)
		return "", nil
	}

	// The window covered rows Offset+1..Offset+Limit past the cursor. Anything
	// beyond that is a row this page never reached, so the emptiness was the ACL.
	//
	// This assumes sysparm_limit is honoured exactly, which an empty page gives no
	// way to confirm. Measured true here (see testdata); if a deployment ever
	// narrowed the window, sysparm_no_count reports its real size.
	next := v.Offset + v.Limit
	if count <= next {
		l.Debug("baton-servicenow: ending the listing, no rows past the window",
			zap.String("url", url),
			zap.String("cursor", v.LastID),
			zap.Int("count", count),
			zap.Int("offset", v.Offset),
		)
		return "", nil
	}
	return EncodeKeysetToken(v.LastID, next)
}

// readTotalCount parses the row count ServiceNow puts in X-Total-Count.
func readTotalCount(header http.Header) (int, bool) {
	raw := header.Get("X-Total-Count")
	if raw == "" {
		return 0, false
	}
	size, err := ConvertPageToken(raw)
	if err != nil {
		return 0, false
	}
	return size, true
}

// Table sys_user (Users). GetUsers always enumerates -- there's no
// per-user provisioning variant -- so the domain filter and its page-size
// cap (domainFilteredPageSize) always apply when AllowedDomains is
// configured.
func (c *Client) GetUsers(ctx context.Context, paginationVars KeysetPaginationVars) ([]User, string, annotations.Annotations, error) {
	paginationVars = cappedForDomainFilter("", c.AllowedDomains, paginationVars)
	return getKeysetPage(ctx, c, c.apiURL(UsersBaseUrl, c.deployment),
		prepareUserFilters(c.AllowedDomains, c.CustomUserFields), &paginationVars,
		func(u User) string { return u.Id })
}

func (c *Client) GetUser(ctx context.Context, userId string) (*User, annotations.Annotations, error) {
	var userResponse UserResponse

	_, annos, err := c.get(
		ctx,
		c.apiURL(UserBaseUrl, c.deployment, userId),
		&userResponse,
		WithFields(UserFields...),
	)

	if err != nil {
		return nil, annos, err
	}

	return &userResponse.Result, annos, nil
}

// Table sys_user_group (Groups).
func (c *Client) GetGroups(ctx context.Context, paginationVars KeysetPaginationVars, groupIDs []string) ([]Group, string, annotations.Annotations, error) {
	return getKeysetPage(ctx, c, c.apiURL(GroupsBaseUrl, c.deployment),
		prepareGroupFilters(groupIDs), &paginationVars,
		func(g Group) string { return g.Id })
}

func (c *Client) GetGroup(ctx context.Context, groupId string) (*Group, annotations.Annotations, error) {
	var groupResponse GroupResponse

	_, annos, err := c.get(
		ctx,
		c.apiURL(GroupBaseUrl, c.deployment, groupId),
		&groupResponse,
		// manager is not in the default GroupFields (the bulk group list
		// doesn't need it); request it here for schedule manager resolution.
		WithFields("sys_id", "description", "name", "manager"),
	)

	if err != nil {
		return nil, annos, err
	}

	return &groupResponse.Result, annos, nil
}

// Table sys_user_grmember (Group Members). When userId is empty
// (enumeration), results are scoped to allowed-domains via user.email and
// the page size is capped (see domainFilteredPageSize).
func (c *Client) GetUserToGroup(ctx context.Context, userId string, groupId string, paginationVars KeysetPaginationVars) ([]GroupMember, string, annotations.Annotations, error) {
	paginationVars = cappedForDomainFilter(userId, c.AllowedDomains, paginationVars)
	return getKeysetPage(ctx, c, c.apiURL(GroupMembersBaseUrl, c.deployment),
		prepareUserToGroupFilter(userId, groupId, c.AllowedDomains), &paginationVars,
		func(m GroupMember) string { return m.Id })
}

func (c *Client) AddUserToGroup(ctx context.Context, record GroupMemberPayload) (annotations.Annotations, error) {
	return c.post(
		ctx,
		c.apiURL(GroupMembersBaseUrl, c.deployment),
		nil,
		&record,
		WithIncludeResponseBody(),
	)
}

func (c *Client) RemoveUserFromGroup(ctx context.Context, id string) (annotations.Annotations, error) {
	return c.delete(
		ctx,
		c.apiURL(GroupMemberDetailBaseUrl, c.deployment, id),
		nil,
	)
}

// Table `cmn_rota_roster` (On-Call Rosters).
func (c *Client) GetRosters(ctx context.Context, paginationVars KeysetPaginationVars) ([]Roster, string, annotations.Annotations, error) {
	return getKeysetPage(ctx, c, c.apiURL(RostersBaseUrl, c.deployment),
		prepareRosterFilters(), &paginationVars,
		func(r Roster) string { return r.Id })
}

// Table `cmn_rota_member` (On-Call Roster Members).
func (c *Client) GetRotaMembers(ctx context.Context, rosterId string, memberId string, paginationVars KeysetPaginationVars) ([]RotaMember, string, annotations.Annotations, error) {
	paginationVars = cappedForDomainFilter(memberId, c.AllowedDomains, paginationVars)
	return getKeysetPage(ctx, c, c.apiURL(RotaMembersBaseUrl, c.deployment),
		prepareRotaMemberFilter(rosterId, memberId, c.AllowedDomains), &paginationVars,
		func(m RotaMember) string { return m.Id })
}

// WhoIsOnCall returns the on-call lineup for a roster now, ordered (Order==1 is
// currently on call). Members must also be in the assignment group to appear.
func (c *Client) WhoIsOnCall(ctx context.Context, rosterId string) ([]OnCallMember, error) {
	var resp WhoIsOnCallResponse

	_, _, err := c.get(
		ctx,
		c.apiURL(WhoIsOnCallUrl, c.deployment),
		&resp,
		WithQueryParam("roster_ids", rosterId),
	)
	if err != nil {
		return nil, err
	}

	return resp.Result, nil
}

// GetRoster fetches a single on-call roster (cmn_rota_roster) by sys_id.
func (c *Client) GetRoster(ctx context.Context, rosterId string) (*Roster, error) {
	var resp RosterResponse
	_, _, err := c.get(
		ctx,
		c.apiURL(RosterDetailBaseUrl, c.deployment, rosterId),
		&resp,
		WithFields("sys_id", "name", "rota"),
	)
	if err != nil {
		return nil, err
	}
	return &resp.Result, nil
}

// GetRota fetches a single on-call rota (cmn_rota) by sys_id.
func (c *Client) GetRota(ctx context.Context, rotaId string) (*Rota, error) {
	var resp RotaResponse
	_, _, err := c.get(
		ctx,
		c.apiURL(RotaDetailBaseUrl, c.deployment, rotaId),
		&resp,
		WithFields("sys_id", "name", "group"),
	)
	if err != nil {
		return nil, err
	}
	return &resp.Result, nil
}

// AddOnCallMember adds a user to roster(s) via the on_call_add_member action
// table; the engine creates the cmn_rota_member row. User must be in the group.
func (c *Client) AddOnCallMember(ctx context.Context, payload OnCallAddMemberPayload) (annotations.Annotations, error) {
	return c.post(
		ctx,
		c.apiURL(OnCallAddMemberUrl, c.deployment),
		nil,
		&payload,
		WithIncludeResponseBody(),
	)
}

// RemoveOnCallMember removes a user from roster(s) via the on_call_remove_member
// action table.
func (c *Client) RemoveOnCallMember(ctx context.Context, payload OnCallRemoveMemberPayload) (annotations.Annotations, error) {
	return c.post(
		ctx,
		c.apiURL(OnCallRemoveMemberUrl, c.deployment),
		nil,
		&payload,
		WithIncludeResponseBody(),
	)
}

// Table sys_user_role (Roles).
func (c *Client) GetRoles(ctx context.Context, paginationVars KeysetPaginationVars) ([]Role, string, annotations.Annotations, error) {
	return getKeysetPage(ctx, c, c.apiURL(RolesBaseUrl, c.deployment),
		prepareRoleFilters(), &paginationVars,
		func(r Role) string { return r.Id })
}

// Table sys_user_has_role (User to Role). When userId is empty
// (enumeration), results are scoped to allowed-domains via user.email and
// the page size is capped (see domainFilteredPageSize).
func (c *Client) GetUserToRole(ctx context.Context, userId string, roleId string, paginationVars KeysetPaginationVars) ([]UserToRole, string, annotations.Annotations, error) {
	paginationVars = cappedForDomainFilter(userId, c.AllowedDomains, paginationVars)
	return getKeysetPage(ctx, c, c.apiURL(UserRolesBaseUrl, c.deployment),
		prepareUserToRoleFilter(userId, roleId, c.AllowedDomains), &paginationVars,
		func(r UserToRole) string { return r.Id })
}

func (c *Client) GrantRoleToUser(ctx context.Context, record UserToRolePayload) (annotations.Annotations, error) {
	return c.post(
		ctx,
		c.apiURL(UserRolesBaseUrl, c.deployment),
		nil,
		&record,
		WithIncludeResponseBody(),
	)
}

func (c *Client) RevokeRoleFromUser(ctx context.Context, id string) (annotations.Annotations, error) {
	return c.delete(
		ctx,
		c.apiURL(UserRoleDetailBaseUrl, c.deployment, id),
		nil,
	)
}

// Table sys_group_has_role (Group to Role). No domain filter -- groups
// don't have an email to scope by.
func (c *Client) GetGroupToRole(ctx context.Context, groupId string, roleId string, paginationVars KeysetPaginationVars) ([]GroupToRole, string, annotations.Annotations, error) {
	return getKeysetPage(ctx, c, c.apiURL(GroupRolesBaseUrl, c.deployment),
		prepareGroupToRoleFilter(groupId, roleId), &paginationVars,
		func(r GroupToRole) string { return r.Id })
}

func (c *Client) GrantRoleToGroup(ctx context.Context, record GroupToRolePayload) (annotations.Annotations, error) {
	return c.post(
		ctx,
		c.apiURL(GroupRolesBaseUrl, c.deployment),
		nil,
		&record,
		WithIncludeResponseBody(),
	)
}

func (c *Client) RevokeRoleFromGroup(ctx context.Context, id string) (annotations.Annotations, error) {
	return c.delete(
		ctx,
		c.apiURL(GroupRoleDetailBaseUrl, c.deployment, id),
		nil,
	)
}

func (c *Client) get(ctx context.Context, urlAddress string, resourceResponse interface{}, reqOptions ...ReqOpt) (string, annotations.Annotations, error) {
	return c.doRequestWithRetry(
		ctx,
		urlAddress,
		http.MethodGet,
		nil,
		&resourceResponse,
		reqOptions...,
	)
}

// getKeyset is like get but for keyset-paginated callers: it never computes
// the legacy Link-header/X-Total-Count token, so it can't fail because of it.
// Returns the headers as well, for X-Total-Count.
func (c *Client) getKeyset(ctx context.Context, urlAddress string, resourceResponse interface{}, reqOptions ...ReqOpt) (http.Header, annotations.Annotations, error) {
	return c.doRequestWithRetryKeyset(
		ctx,
		urlAddress,
		http.MethodGet,
		nil,
		&resourceResponse,
		reqOptions...,
	)
}

func (c *Client) post(
	ctx context.Context,
	urlAddress string,
	resourceResponse interface{},
	data interface{},
	requestOptions ...ReqOpt,
) (annotations.Annotations, error) {
	_, annos, err := c.doRequestWithRetry(
		ctx,
		urlAddress,
		http.MethodPost,
		data,
		&resourceResponse,
		requestOptions...,
	)

	return annos, err
}

func (c *Client) patch(
	ctx context.Context,
	urlAddress string,
	resourceResponse interface{},
	data interface{},
	requestOptions ...ReqOpt,
) (annotations.Annotations, error) {
	_, annos, err := c.doRequestWithRetry(
		ctx,
		urlAddress,
		http.MethodPatch,
		data,
		&resourceResponse,
		requestOptions...,
	)

	return annos, err
}

func (c *Client) delete(
	ctx context.Context,
	urlAddress string,
	resourceResponse interface{},
	reqOptions ...ReqOpt,
) (annotations.Annotations, error) {
	_, annos, err := c.doRequestWithRetry(
		ctx,
		urlAddress,
		http.MethodDelete,
		nil,
		&resourceResponse,
		reqOptions...,
	)

	return annos, err
}

// IsInvalidTableError reports whether err is ServiceNow's HTTP 400 "Invalid
// table" error, returned when a queried table doesn't exist — e.g. the on-call
// tables when the On-Call Scheduling plugin isn't installed.
func IsInvalidTableError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Invalid table")
}

// IsAccessDeniedError reports whether err is a ServiceNow HTTP 403, returned
// when the table exists but the account lacks the read ACL for it.
func IsAccessDeniedError(err error) bool {
	return status.Code(err) == codes.PermissionDenied
}

// doRequest performs the request, decodes a successful JSON body into
// resourceResponse, and returns the legacy Link-header/X-Total-Count
// offset-pagination token used by Service Catalog/ticketing callers. Keyset
// callers use doRequestWithRetryKeyset instead.
func (c *Client) doRequest(ctx context.Context, urlAddress string, method string, data any, resourceResponse any, reqOptions ...ReqOpt) (string, annotations.Annotations, error) {
	header, annos, err := c.doHTTPRequest(ctx, urlAddress, method, data, resourceResponse, reqOptions...)
	if err != nil {
		return "", annos, err
	}
	token, err := legacyOffsetToken(header)
	return token, annos, err
}

// doHTTPRequest sends the request through uhttp and, for non-DELETE methods,
// decodes a successful JSON body into resourceResponse. Returns the response
// headers (for callers that read pagination headers afterward) and the
// rate-limit annotations extracted from the response.
//
// uhttp.BaseHttpClient.Do owns status-to-gRPC-code mapping
// (uhttp.GrpcCodeFromHTTPStatus) and already attaches the rate-limit detail
// to the error it returns, so there is no connector-local status table.
func (c *Client) doHTTPRequest(ctx context.Context, urlAddress string, method string, data any, resourceResponse any, reqOptions ...ReqOpt) (http.Header, annotations.Annotations, error) {
	var body io.Reader

	if data != nil {
		jsonBody, err := json.Marshal(data)
		if err != nil {
			return nil, nil, err
		}

		body = bytes.NewBuffer(jsonBody)
	}

	req, err := http.NewRequestWithContext(ctx, method, urlAddress, body)
	if err != nil {
		return nil, nil, err
	}

	// Set default value
	WithQueryParam("sysparm_exclude_reference_link", "true")(req)

	req.Header.Set("Authorization", c.auth)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	// uhttp caches GET 200s in memory for an hour by default
	// (uhttp.DefaultCacheConfig). Identity and membership data must be read
	// fresh on every sync -- a cached page would resurface deleted grants,
	// so opt out rather than inherit the default.
	req.Header.Set("Cache-Control", "no-cache")
	if method == http.MethodPost || method == http.MethodPatch {
		req.Header.Set("X-no-response-body", "true")
	}

	for _, o := range reqOptions {
		o(req)
	}

	req.URL.RawQuery = req.URL.Query().Encode()

	// Rate-limit data is captured for every response, success or failure.
	// The success path is the one that matters: it lets the SDK's limiter
	// pace upcoming requests and avoid the next 429, rather than only
	// reacting once one has already been returned.
	var rlData v2.RateLimitDescription
	rawResponse, err := c.httpClient.Do(req, uhttp.WithRatelimitData(&rlData))
	if rawResponse != nil {
		// uhttp has already drained and closed the network body, swapping in
		// an in-memory buffer, so this close is a no-op today. Kept nil-guarded
		// and unconditional so the error path below can still read the body and
		// the handling stays correct if uhttp's buffering ever changes.
		defer rawResponse.Body.Close()
	}
	annos := annotations.Annotations{}
	annos.WithRateLimiting(&rlData)

	if err != nil {
		switch {
		case rawResponse == nil:
			// Transport-level failure -- no response to salvage.
			return nil, annos, err

		case rawResponse.StatusCode >= 300:
			// Best-effort read: the body only enriches the message, so a
			// read failure must not mask the status the caller needs. uhttp
			// has already replaced Body with a re-readable buffer.
			respBody, _ := io.ReadAll(rawResponse.Body)
			return nil, annos, fmt.Errorf("request failed with status %d: %s: %w", rawResponse.StatusCode, string(respBody), err)

		default:
			// A 2xx with a non-nil error means a DoOption failed, not the
			// request. The only option we pass is rate-limit extraction, which
			// is advisory: ExtractRateLimitData runs strconv.ParseInt over the
			// limit/remaining headers, so a non-numeric value ("unlimited")
			// errors out. Failing a successfully fetched page over a header we
			// only use for pacing would be strictly worse than ignoring it.
			ctxzap.Extract(ctx).Warn(
				"baton-servicenow: ignoring rate limit extraction failure on a successful response",
				zap.Int("status_code", rawResponse.StatusCode),
				zap.Error(err),
			)
		}
	}

	if method != http.MethodDelete {
		if err := json.NewDecoder(rawResponse.Body).Decode(&resourceResponse); err != nil {
			// A hibernating ServiceNow instance answers 200 with an HTML
			// page, and maintenance pages / WAF interstitials in front of a
			// live one do the same. Decoding that as JSON yields "invalid
			// character '<'", which tells an operator nothing about what is
			// actually wrong. Only the message changes here -- the decode
			// still has to fail first, so no working response is affected.
			if contentType := rawResponse.Header.Get("Content-Type"); !uhttp.IsJSONContentType(contentType) {
				return nil, annos, fmt.Errorf(
					"expected a JSON response but got status %d with content-type %q -- the ServiceNow instance may be hibernating or behind an error page: %w",
					rawResponse.StatusCode, contentType, err,
				)
			}
			return nil, annos, fmt.Errorf("decode %s response: %w", method, err)
		}
	}

	return rawResponse.Header, annos, nil
}

// legacyOffsetToken computes the Service Catalog/ticketing offset-pagination
// token from the Link/X-Total-Count headers of an already-completed
// request. Only doRequest's (non-keyset) callers use this.
func legacyOffsetToken(header http.Header) (string, error) {
	totalCountHeader := header.Get("X-Total-Count")
	totalCount, err := ConvertPageToken(totalCountHeader)
	if err != nil {
		return "", err
	}

	var pageToken string
	pagingLinks := linkheader.Parse(header.Get("Link"))
	for _, link := range pagingLinks {
		if link.Rel == "next" {
			nextPageUrl, err := url.Parse(link.URL)
			if err != nil {
				return "", err
			}
			offset := nextPageUrl.Query().Get("sysparm_offset")
			token, err := ConvertPageToken(offset)
			if err != nil {
				return "", err
			}
			if token < totalCount {
				pageToken = offset
			}
			break
		}
	}

	return pageToken, nil
}

const (
	maxAuthRetries    = 3
	authRetryBaseWait = time.Second
)

// doRequestWithRetry wraps doRequest with a small retry loop for transient 401 responses.
// On each 401 it logs the ServiceNow error body and waits an increasing delay before retrying.
func (c *Client) doRequestWithRetry(ctx context.Context, urlAddress string, method string, data any, resourceResponse any, reqOptions ...ReqOpt) (string, annotations.Annotations, error) {
	return withAuthRetry(ctx, urlAddress, method, func() (string, annotations.Annotations, error) {
		return c.doRequest(ctx, urlAddress, method, data, resourceResponse, reqOptions...)
	})
}

// doRequestWithRetryKeyset is doRequestWithRetry for keyset callers. It skips
// doRequest's legacy Link-header token, which could otherwise fail a page that
// decoded fine, and hands back the headers for X-Total-Count.
func (c *Client) doRequestWithRetryKeyset(ctx context.Context, urlAddress string, method string, data any, resourceResponse any, reqOptions ...ReqOpt) (http.Header, annotations.Annotations, error) {
	var header http.Header
	_, annos, err := withAuthRetry(ctx, urlAddress, method, func() (string, annotations.Annotations, error) {
		h, a, reqErr := c.doHTTPRequest(ctx, urlAddress, method, data, resourceResponse, reqOptions...)
		header = h
		return "", a, reqErr
	})
	return header, annos, err
}

// withAuthRetry retries attempt on transient 401 responses, logging the
// retry/backoff around it. attempt returns whatever pagination token its
// underlying request produces (keyset callers always pass "") plus the
// rate-limit annotations, which are propagated from the final attempt --
// including the failing one, so a caller that gives up still reports what
// the last response said about the limit.
func withAuthRetry(ctx context.Context, urlAddress string, method string, attempt func() (string, annotations.Annotations, error)) (string, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	var lastErr error
	var lastAnnos annotations.Annotations
	for try := 1; try <= maxAuthRetries+1; try++ {
		if try > 1 {
			delay := time.Duration(try-1) * authRetryBaseWait
			l.Debug("baton-servicenow: retrying request after 401",
				zap.String("url", urlAddress),
				zap.String("method", method),
				zap.Int("attempt", try),
				zap.Int("max_attempts", maxAuthRetries+1),
				zap.Duration("delay", delay),
			)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return "", lastAnnos, ctx.Err()
			}
		}

		pageToken, annos, err := attempt()
		lastAnnos = annos
		if err == nil {
			if try > 1 {
				l.Debug("baton-servicenow: request succeeded after retry",
					zap.String("url", urlAddress),
					zap.String("method", method),
					zap.Int("attempt", try),
				)
			}
			return pageToken, annos, nil
		}

		if status.Code(err) != codes.Unauthenticated {
			return "", annos, err
		}

		l.Debug("baton-servicenow: received 401 unauthorized",
			zap.String("url", urlAddress),
			zap.String("method", method),
			zap.Int("attempt", try),
			zap.Int("max_attempts", maxAuthRetries+1),
			zap.Error(err),
		)
		lastErr = err
	}

	l.Debug("baton-servicenow: request failed after all retry attempts",
		zap.String("url", urlAddress),
		zap.String("method", method),
		zap.Int("max_attempts", maxAuthRetries+1),
		zap.Error(lastErr),
	)
	return "", lastAnnos, lastErr
}

func (c *Client) CreateUserAccount(ctx context.Context, user any) (*User, annotations.Annotations, error) {
	var response UserResponse

	annos, err := c.post(
		ctx,
		c.apiURL(UsersBaseUrl, c.deployment),
		&response,
		user,
		WithIncludeResponseBody(),
	)

	if err != nil {
		return nil, annos, fmt.Errorf("failed to create user in ServiceNow: %w", err)
	}

	return &response.Result, annos, nil
}

func (c *Client) UpdateUserActiveStatus(ctx context.Context, userId string, active bool) (*User, annotations.Annotations, error) {
	payload := map[string]bool{
		"active": active,
	}

	var response UserResponse
	annos, err := c.patch(
		ctx,
		c.apiURL(UserBaseUrl, c.deployment, userId),
		&response,
		payload,
		WithIncludeResponseBody(),
	)

	if err != nil {
		return nil, annos, fmt.Errorf("failed to update user active status in ServiceNow: %w", err)
	}

	return &response.Result, annos, nil
}

// Includes variables that come from variable sets (Table API -> item_option_new) and choices for those set variables.
func (c *Client) GetCatalogItemVariablesPlusSets(ctx context.Context, itemSysID string) ([]CatalogItemVariable, annotations.Annotations, error) {
	itemVars, annos, err := c.GetCatalogItemVariables(ctx, itemSysID)
	if err != nil {
		return nil, annos, fmt.Errorf("failed to get item variables: %w", err)
	}

	// Find attached variable sets
	links, _, annos, err := c.GetVariableSetLinksForItem(ctx, itemSysID, PaginationVars{Limit: 200})
	if err != nil {
		return nil, annos, fmt.Errorf("failed to get variable set links: %w", err)
	}
	if len(links) == 0 {
		return itemVars, annos, nil // nothing to add
	}

	setIDs := make([]string, 0, len(links))
	for _, l := range links {
		setIDs = append(setIDs, l.VariableSet)
	}

	// Fetch variables that belong to those sets
	setVars, _, annos, err := c.GetVariablesBySetIDs(ctx, setIDs, PaginationVars{Limit: 500})
	if err != nil {
		return nil, annos, fmt.Errorf("failde to get variables by set ids: %w", err)
	}

	// Fetch choices for set variables (so selects have options)
	varIDs := make([]string, 0, len(setVars))
	for _, v := range setVars {
		varIDs = append(varIDs, v.SysID)
	}
	choices, _, annos, err := c.GetChoicesForVariables(ctx, varIDs, PaginationVars{Limit: 1000})
	if err != nil {
		return nil, annos, fmt.Errorf("failed to get choices for set variables: %w", err)
	}
	choicesByQ := make(map[string][]QuestionChoice, len(varIDs))
	for _, ch := range choices {
		choicesByQ[ch.Question] = append(choicesByQ[ch.Question], ch)
	}

	// Map set variables to CatalogItemVariable
	cvSet := make([]CatalogItemVariable, 0, len(setVars))
	for _, v := range setVars {
		cvSet = append(cvSet, MapItemOptionNewToCatalogItemVariable(v, choicesByQ[v.SysID]))
	}

	// Merge (prefer direct items on ID collisions)
	out := make([]CatalogItemVariable, 0, len(itemVars)+len(cvSet))
	seen := make(map[string]struct{}, len(itemVars))
	for _, v := range itemVars {
		out = append(out, v)
		seen[v.ID] = struct{}{}
	}
	for _, v := range cvSet {
		if _, dup := seen[v.ID]; !dup {
			out = append(out, v)
		}
	}

	return out, annos, nil
}

func (c *Client) GetVariableSetLinksForItem(ctx context.Context, itemSysID string, pg PaginationVars) ([]VariableSetM2M, string, annotations.Annotations, error) {
	var resp VariableSetM2MResponse
	req := []ReqOpt{
		WithQueryParam("sysparm_query", fmt.Sprintf("sc_cat_item=%s", itemSysID)),
		WithQueryParam("sysparm_fields", "sys_id,variable_set"),
		WithQueryParam("sysparm_exclude_reference_link", "true"),
	}
	req = append(req, paginationVarsToReqOptions(&pg)...)

	next, annos, err := c.get(ctx, c.apiURL(VariableSetM2MBaseUrl, c.deployment), &resp, req...)
	if err != nil {
		return nil, "", annos, err
	}
	return resp.Result, next, annos, nil
}

func (c *Client) GetVariablesBySetIDs(ctx context.Context, setIDs []string, pg PaginationVars) ([]ItemOptionNew, string, annotations.Annotations, error) {
	if len(setIDs) == 0 {
		return nil, "", nil, nil
	}
	var resp ItemOptionNewResponse
	req := []ReqOpt{
		WithQueryParam("sysparm_query", "variable_setIN"+strings.Join(setIDs, ",")),
		WithQueryParam("sysparm_fields", "sys_id,name,question_text,type,mandatory,default_value,reference,attributes,active,cat_item,variable_set"),
		WithQueryParam("sysparm_exclude_reference_link", "true"),
	}
	req = append(req, paginationVarsToReqOptions(&pg)...)

	next, annos, err := c.get(ctx, c.apiURL(ItemOptionNewBaseUrl, c.deployment), &resp, req...)
	if err != nil {
		return nil, "", annos, err
	}
	return resp.Result, next, annos, nil
}

func (c *Client) GetChoicesForVariables(ctx context.Context, varIDs []string, pg PaginationVars) ([]QuestionChoice, string, annotations.Annotations, error) {
	if len(varIDs) == 0 {
		return nil, "", nil, nil
	}
	var resp QuestionChoiceResponse
	req := []ReqOpt{
		WithQueryParam("sysparm_query", "questionIN"+strings.Join(varIDs, ",")),
		WithQueryParam("sysparm_fields", "sys_id,label,value,question"),
		WithQueryParam("sysparm_exclude_reference_link", "true"),
	}
	req = append(req, paginationVarsToReqOptions(&pg)...)

	next, annos, err := c.get(ctx, c.apiURL(QuestionChoiceBaseUrl, c.deployment), &resp, req...)
	if err != nil {
		return nil, "", annos, err
	}
	return resp.Result, next, annos, nil
}

// Unused but consider switching to this to get both direct catalog item variables and variables from variable sets.
func (c *Client) GetVariablesForItem(ctx context.Context, itemSysID string, pg PaginationVars) ([]ItemOptionNew, string, annotations.Annotations, error) {
	var resp ItemOptionNewResponse
	req := []ReqOpt{
		WithQueryParam("sysparm_query", fmt.Sprintf("cat_item=%s", itemSysID)),
		WithQueryParam("sysparm_fields", "sys_id,name,question_text,type,mandatory,default_value,reference,attributes,active,cat_item,variable_set"),
		WithQueryParam("sysparm_exclude_reference_link", "true"),
	}
	req = append(req, paginationVarsToReqOptions(&pg)...)

	next, annos, err := c.get(ctx, c.apiURL(ItemOptionNewBaseUrl, c.deployment), &resp, req...)
	if err != nil {
		return nil, "", annos, err
	}
	return resp.Result, next, annos, nil
}
