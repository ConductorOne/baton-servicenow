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

// getKeysetPage runs one keyset-paginated Table API GET and derives the
// next seek token from the response. Uses c.getKeyset, not c.get: keyset
// callers compute their own token via nextKeysetToken and don't need
// doRequest's legacy Link-header token.
func getKeysetPage[T any](ctx context.Context, c *Client, url string, reqOpts []ReqOpt, idFn func(T) string) ([]T, string, annotations.Annotations, error) {
	var resp ListResponse[T]
	annos, err := c.getKeyset(ctx, url, &resp, reqOpts...)
	if err != nil {
		return nil, "", annos, err
	}
	return resp.Result, nextKeysetToken(resp.Result, idFn), annos, nil
}

// Table sys_user (Users). GetUsers always enumerates -- there's no
// per-user provisioning variant -- so the domain filter and its page-size
// cap (domainFilteredPageSize) always apply when AllowedDomains is
// configured.
func (c *Client) GetUsers(ctx context.Context, paginationVars KeysetPaginationVars) ([]User, string, annotations.Annotations, error) {
	paginationVars = cappedForDomainFilter("", c.AllowedDomains, paginationVars)
	reqOpts := buildKeysetReqOptions(prepareUserFilters(c.AllowedDomains, c.CustomUserFields), &paginationVars)

	return getKeysetPage(ctx, c, c.apiURL(UsersBaseUrl, c.deployment), reqOpts, func(u User) string { return u.Id })
}

func (c *Client) GetUser(ctx context.Context, userId string) (*User, error) {
	var userResponse UserResponse

	_, _, err := c.get(
		ctx,
		c.apiURL(UserBaseUrl, c.deployment, userId),
		&userResponse,
		WithFields(UserFields...),
	)

	if err != nil {
		return nil, err
	}

	return &userResponse.Result, nil
}

// Table sys_user_group (Groups).
func (c *Client) GetGroups(ctx context.Context, paginationVars KeysetPaginationVars, groupIDs []string) ([]Group, string, annotations.Annotations, error) {
	reqOpts := buildKeysetReqOptions(prepareGroupFilters(groupIDs), &paginationVars)

	return getKeysetPage(ctx, c, c.apiURL(GroupsBaseUrl, c.deployment), reqOpts, func(g Group) string { return g.Id })
}

func (c *Client) GetGroup(ctx context.Context, groupId string) (*Group, error) {
	var groupResponse GroupResponse

	_, _, err := c.get(
		ctx,
		c.apiURL(GroupBaseUrl, c.deployment, groupId),
		&groupResponse,
		WithFields(GroupFields...),
	)

	if err != nil {
		return nil, err
	}

	return &groupResponse.Result, nil
}

// Table sys_user_grmember (Group Members). When userId is empty
// (enumeration), results are scoped to allowed-domains via user.email and
// the page size is capped (see domainFilteredPageSize).
func (c *Client) GetUserToGroup(ctx context.Context, userId string, groupId string, paginationVars KeysetPaginationVars) ([]GroupMember, string, annotations.Annotations, error) {
	paginationVars = cappedForDomainFilter(userId, c.AllowedDomains, paginationVars)
	reqOpts := buildKeysetReqOptions(prepareUserToGroupFilter(userId, groupId, c.AllowedDomains), &paginationVars)

	return getKeysetPage(ctx, c, c.apiURL(GroupMembersBaseUrl, c.deployment), reqOpts, func(m GroupMember) string { return m.Id })
}

func (c *Client) AddUserToGroup(ctx context.Context, record GroupMemberPayload) error {
	return c.post(
		ctx,
		c.apiURL(GroupMembersBaseUrl, c.deployment),
		nil,
		&record,
		WithIncludeResponseBody(),
	)
}

func (c *Client) RemoveUserFromGroup(ctx context.Context, id string) error {
	return c.delete(
		ctx,
		c.apiURL(GroupMemberDetailBaseUrl, c.deployment, id),
		nil,
	)
}

// Table sys_user_role (Roles).
func (c *Client) GetRoles(ctx context.Context, paginationVars KeysetPaginationVars) ([]Role, string, annotations.Annotations, error) {
	reqOpts := buildKeysetReqOptions(prepareRoleFilters(), &paginationVars)

	return getKeysetPage(ctx, c, c.apiURL(RolesBaseUrl, c.deployment), reqOpts, func(r Role) string { return r.Id })
}

// Table sys_user_has_role (User to Role). When userId is empty
// (enumeration), results are scoped to allowed-domains via user.email and
// the page size is capped (see domainFilteredPageSize).
func (c *Client) GetUserToRole(ctx context.Context, userId string, roleId string, paginationVars KeysetPaginationVars) ([]UserToRole, string, annotations.Annotations, error) {
	paginationVars = cappedForDomainFilter(userId, c.AllowedDomains, paginationVars)
	reqOpts := buildKeysetReqOptions(prepareUserToRoleFilter(userId, roleId, c.AllowedDomains), &paginationVars)

	return getKeysetPage(ctx, c, c.apiURL(UserRolesBaseUrl, c.deployment), reqOpts, func(r UserToRole) string { return r.Id })
}

func (c *Client) GrantRoleToUser(ctx context.Context, record UserToRolePayload) error {
	return c.post(
		ctx,
		c.apiURL(UserRolesBaseUrl, c.deployment),
		nil,
		&record,
		WithIncludeResponseBody(),
	)
}

func (c *Client) RevokeRoleFromUser(ctx context.Context, id string) error {
	return c.delete(
		ctx,
		c.apiURL(UserRoleDetailBaseUrl, c.deployment, id),
		nil,
	)
}

// Table sys_group_has_role (Group to Role). No domain filter -- groups
// don't have an email to scope by.
func (c *Client) GetGroupToRole(ctx context.Context, groupId string, roleId string, paginationVars KeysetPaginationVars) ([]GroupToRole, string, annotations.Annotations, error) {
	reqOpts := buildKeysetReqOptions(prepareGroupToRoleFilter(groupId, roleId), &paginationVars)

	return getKeysetPage(ctx, c, c.apiURL(GroupRolesBaseUrl, c.deployment), reqOpts, func(r GroupToRole) string { return r.Id })
}

func (c *Client) GrantRoleToGroup(ctx context.Context, record GroupToRolePayload) error {
	return c.post(
		ctx,
		c.apiURL(GroupRolesBaseUrl, c.deployment),
		nil,
		&record,
		WithIncludeResponseBody(),
	)
}

func (c *Client) RevokeRoleFromGroup(ctx context.Context, id string) error {
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
func (c *Client) getKeyset(ctx context.Context, urlAddress string, resourceResponse interface{}, reqOptions ...ReqOpt) (annotations.Annotations, error) {
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
) error {
	_, _, err := c.doRequestWithRetry(
		ctx,
		urlAddress,
		http.MethodPost,
		data,
		&resourceResponse,
		requestOptions...,
	)

	return err
}

func (c *Client) patch(
	ctx context.Context,
	urlAddress string,
	resourceResponse interface{},
	data interface{},
	requestOptions ...ReqOpt,
) error {
	_, _, err := c.doRequestWithRetry(
		ctx,
		urlAddress,
		http.MethodPatch,
		data,
		&resourceResponse,
		requestOptions...,
	)

	return err
}

func (c *Client) delete(
	ctx context.Context,
	urlAddress string,
	resourceResponse interface{},
	reqOptions ...ReqOpt,
) error {
	_, _, err := c.doRequestWithRetry(
		ctx,
		urlAddress,
		http.MethodDelete,
		nil,
		&resourceResponse,
		reqOptions...,
	)

	return err
}

// doRequest performs the request, decodes a successful JSON body into
// resourceResponse, and returns the legacy Link-header/X-Total-Count
// offset-pagination token used by Service Catalog/ticketing callers. Keyset
// callers use doRequestKeyset instead.
func (c *Client) doRequest(ctx context.Context, urlAddress string, method string, data any, resourceResponse any, reqOptions ...ReqOpt) (string, annotations.Annotations, error) {
	header, annos, err := c.doHTTPRequest(ctx, urlAddress, method, data, resourceResponse, reqOptions...)
	if err != nil {
		return "", annos, err
	}
	token, err := legacyOffsetToken(header)
	return token, annos, err
}

// doRequestKeyset is doRequest without the legacy Link-header/X-Total-Count
// token computation. Keyset callers derive their own token from the decoded
// body (nextKeysetToken), so a malformed Link header or non-numeric
// sysparm_offset must not discard an otherwise successfully decoded page.
func (c *Client) doRequestKeyset(ctx context.Context, urlAddress string, method string, data any, resourceResponse any, reqOptions ...ReqOpt) (annotations.Annotations, error) {
	_, annos, err := c.doHTTPRequest(ctx, urlAddress, method, data, resourceResponse, reqOptions...)
	return annos, err
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
		if rawResponse != nil && rawResponse.StatusCode >= 300 {
			// Best-effort read: the body only enriches the message, so a
			// read failure must not mask the status the caller needs. uhttp
			// has already replaced Body with a re-readable buffer.
			respBody, _ := io.ReadAll(rawResponse.Body)
			return nil, annos, fmt.Errorf("request failed with status %d: %s: %w", rawResponse.StatusCode, string(respBody), err)
		}
		return nil, annos, err
	}

	if method != http.MethodDelete {
		if err := json.NewDecoder(rawResponse.Body).Decode(&resourceResponse); err != nil {
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

// doRequestWithRetryKeyset is doRequestWithRetry for keyset callers, routed
// through doRequestKeyset instead of doRequest so a legacy-pagination-header
// hiccup can't fail an otherwise successfully decoded page.
func (c *Client) doRequestWithRetryKeyset(ctx context.Context, urlAddress string, method string, data any, resourceResponse any, reqOptions ...ReqOpt) (annotations.Annotations, error) {
	_, annos, err := withAuthRetry(ctx, urlAddress, method, func() (string, annotations.Annotations, error) {
		a, err := c.doRequestKeyset(ctx, urlAddress, method, data, resourceResponse, reqOptions...)
		return "", a, err
	})
	return annos, err
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

func (c *Client) CreateUserAccount(ctx context.Context, user any) (*User, error) {
	var response UserResponse

	err := c.post(
		ctx,
		c.apiURL(UsersBaseUrl, c.deployment),
		&response,
		user,
		WithIncludeResponseBody(),
	)

	if err != nil {
		return nil, fmt.Errorf("failed to create user in ServiceNow: %w", err)
	}

	return &response.Result, nil
}

func (c *Client) UpdateUserActiveStatus(ctx context.Context, userId string, active bool) (*User, error) {
	payload := map[string]bool{
		"active": active,
	}

	var response UserResponse
	err := c.patch(
		ctx,
		c.apiURL(UserBaseUrl, c.deployment, userId),
		&response,
		payload,
		WithIncludeResponseBody(),
	)

	if err != nil {
		return nil, fmt.Errorf("failed to update user active status in ServiceNow: %w", err)
	}

	return &response.Result, nil
}

// Includes variables that come from variable sets (Table API -> item_option_new) and choices for those set variables.
func (c *Client) GetCatalogItemVariablesPlusSets(ctx context.Context, itemSysID string) ([]CatalogItemVariable, error) {
	itemVars, err := c.GetCatalogItemVariables(ctx, itemSysID)
	if err != nil {
		return nil, fmt.Errorf("failed to get item variables: %w", err)
	}

	// Find attached variable sets
	links, _, err := c.GetVariableSetLinksForItem(ctx, itemSysID, PaginationVars{Limit: 200})
	if err != nil {
		return nil, fmt.Errorf("failed to get variable set links: %w", err)
	}
	if len(links) == 0 {
		return itemVars, nil // nothing to add
	}

	setIDs := make([]string, 0, len(links))
	for _, l := range links {
		setIDs = append(setIDs, l.VariableSet)
	}

	// Fetch variables that belong to those sets
	setVars, _, err := c.GetVariablesBySetIDs(ctx, setIDs, PaginationVars{Limit: 500})
	if err != nil {
		return nil, fmt.Errorf("failde to get variables by set ids: %w", err)
	}

	// Fetch choices for set variables (so selects have options)
	varIDs := make([]string, 0, len(setVars))
	for _, v := range setVars {
		varIDs = append(varIDs, v.SysID)
	}
	choices, _, err := c.GetChoicesForVariables(ctx, varIDs, PaginationVars{Limit: 1000})
	if err != nil {
		return nil, fmt.Errorf("failed to get choices for set variables: %w", err)
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

	return out, nil
}

func (c *Client) GetVariableSetLinksForItem(ctx context.Context, itemSysID string, pg PaginationVars) ([]VariableSetM2M, string, error) {
	var resp VariableSetM2MResponse
	req := []ReqOpt{
		WithQueryParam("sysparm_query", fmt.Sprintf("sc_cat_item=%s", itemSysID)),
		WithQueryParam("sysparm_fields", "sys_id,variable_set"),
		WithQueryParam("sysparm_exclude_reference_link", "true"),
	}
	req = append(req, paginationVarsToReqOptions(&pg)...)

	next, _, err := c.get(ctx, c.apiURL(VariableSetM2MBaseUrl, c.deployment), &resp, req...)
	if err != nil {
		return nil, "", err
	}
	return resp.Result, next, nil
}

func (c *Client) GetVariablesBySetIDs(ctx context.Context, setIDs []string, pg PaginationVars) ([]ItemOptionNew, string, error) {
	if len(setIDs) == 0 {
		return nil, "", nil
	}
	var resp ItemOptionNewResponse
	req := []ReqOpt{
		WithQueryParam("sysparm_query", "variable_setIN"+strings.Join(setIDs, ",")),
		WithQueryParam("sysparm_fields", "sys_id,name,question_text,type,mandatory,default_value,reference,attributes,active,cat_item,variable_set"),
		WithQueryParam("sysparm_exclude_reference_link", "true"),
	}
	req = append(req, paginationVarsToReqOptions(&pg)...)

	next, _, err := c.get(ctx, c.apiURL(ItemOptionNewBaseUrl, c.deployment), &resp, req...)
	if err != nil {
		return nil, "", err
	}
	return resp.Result, next, nil
}

func (c *Client) GetChoicesForVariables(ctx context.Context, varIDs []string, pg PaginationVars) ([]QuestionChoice, string, error) {
	if len(varIDs) == 0 {
		return nil, "", nil
	}
	var resp QuestionChoiceResponse
	req := []ReqOpt{
		WithQueryParam("sysparm_query", "questionIN"+strings.Join(varIDs, ",")),
		WithQueryParam("sysparm_fields", "sys_id,label,value,question"),
		WithQueryParam("sysparm_exclude_reference_link", "true"),
	}
	req = append(req, paginationVarsToReqOptions(&pg)...)

	next, _, err := c.get(ctx, c.apiURL(QuestionChoiceBaseUrl, c.deployment), &resp, req...)
	if err != nil {
		return nil, "", err
	}
	return resp.Result, next, nil
}

// Unused but consider switching to this to get both direct catalog item variables and variables from variable sets.
func (c *Client) GetVariablesForItem(ctx context.Context, itemSysID string, pg PaginationVars) ([]ItemOptionNew, string, error) {
	var resp ItemOptionNewResponse
	req := []ReqOpt{
		WithQueryParam("sysparm_query", fmt.Sprintf("cat_item=%s", itemSysID)),
		WithQueryParam("sysparm_fields", "sys_id,name,question_text,type,mandatory,default_value,reference,attributes,active,cat_item,variable_set"),
		WithQueryParam("sysparm_exclude_reference_link", "true"),
	}
	req = append(req, paginationVarsToReqOptions(&pg)...)

	next, _, err := c.get(ctx, c.apiURL(ItemOptionNewBaseUrl, c.deployment), &resp, req...)
	if err != nil {
		return nil, "", err
	}
	return resp.Result, next, nil
}
