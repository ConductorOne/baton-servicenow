package servicenow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

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

	// On-Call member provisioning action tables (the engine processes these
	// server-side and sets the read-only cmn_rota_member.roster field).
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
type UsersResponse = ListResponse[User]
type RolesResponse = ListResponse[Role]
type GroupsResponse = ListResponse[Group]
type GroupResponse = SingleResponse[Group]
type GroupMembersResponse = ListResponse[GroupMember]
type RotaResponse = SingleResponse[Rota]
type RosterResponse = SingleResponse[Roster]
type RostersResponse = ListResponse[Roster]
type RotaMembersResponse = ListResponse[RotaMember]
type WhoIsOnCallResponse = ListResponse[OnCallMember]
type UserRolesResponse = SingleResponse[UserRoles]
type UserToRoleResponse ListResponse[UserToRole]
type GroupToRoleResponse ListResponse[GroupToRole]
type CatalogsResponse = ListResponse[Catalog]
type CategoriesResponse = ListResponse[Category]
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
	httpClient          *http.Client
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
	httpClient *http.Client,
	auth string,
	deployment string,
	ticketSchemaFilters map[string]string,
	allowedDomains []string,
	customUserFields []string,
	baseURLOverride string,
) (*Client, error) {
	var baseURL string
	if baseURLOverride != "" {
		baseURL = baseURLOverride
	} else {
		var err error
		baseURL, err = GenerateURL(InstanceURLTemplate, map[string]string{"Deployment": deployment})
		if err != nil {
			return nil, err
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

// Table `sys_user` (Users).
func (c *Client) GetUsers(ctx context.Context, paginationVars PaginationVars) ([]User, string, error) {
	var usersResponse UsersResponse

	reqOpts := filterToReqOptions(prepareUserFilters(c.AllowedDomains, c.CustomUserFields))
	reqOpts = append(reqOpts, paginationVarsToReqOptions(&paginationVars)...)

	nextPage, err := c.get(
		ctx,
		c.apiURL(UsersBaseUrl, c.deployment),
		&usersResponse,
		reqOpts...,
	)

	if err != nil {
		return nil, "", err
	}

	return usersResponse.Result, nextPage, nil
}

func (c *Client) GetUser(ctx context.Context, userId string) (*User, error) {
	var userResponse UserResponse

	_, err := c.get(
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

// Table `sys_user_group` (Groups).
func (c *Client) GetGroups(ctx context.Context, paginationVars PaginationVars, groupIDs []string) ([]Group, string, error) {
	var groupsResponse GroupsResponse

	reqOpts := filterToReqOptions(prepareGroupFilters(groupIDs))
	reqOpts = append(reqOpts, paginationVarsToReqOptions(&paginationVars)...)
	nextPageToken, err := c.get(
		ctx,
		c.apiURL(GroupsBaseUrl, c.deployment),
		&groupsResponse,
		reqOpts...,
	)

	if err != nil {
		return nil, "", err
	}

	return groupsResponse.Result, nextPageToken, nil
}

func (c *Client) GetGroup(ctx context.Context, groupId string) (*Group, error) {
	var groupResponse GroupResponse

	_, err := c.get(
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

// Table `sys_user_grmember` (Group Members).
func (c *Client) GetUserToGroup(ctx context.Context, userId string, groupId string, paginationVars PaginationVars) ([]GroupMember, string, error) {
	var groupMembersResponse GroupMembersResponse

	reqOpts := filterToReqOptions(prepareUserToGroupFilter(userId, groupId))
	reqOpts = append(reqOpts, paginationVarsToReqOptions(&paginationVars)...)

	nextPageToken, err := c.get(
		ctx,
		c.apiURL(GroupMembersBaseUrl, c.deployment),
		&groupMembersResponse,
		reqOpts...,
	)

	if err != nil {
		return nil, "", err
	}

	return groupMembersResponse.Result, nextPageToken, nil
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

// Table `cmn_rota_roster` (On-Call Rosters).
func (c *Client) GetRosters(ctx context.Context, paginationVars PaginationVars) ([]Roster, string, error) {
	var rostersResponse RostersResponse

	reqOpts := filterToReqOptions(prepareRosterFilters())
	reqOpts = append(reqOpts, paginationVarsToReqOptions(&paginationVars)...)

	nextPageToken, err := c.get(
		ctx,
		c.apiURL(RostersBaseUrl, c.deployment),
		&rostersResponse,
		reqOpts...,
	)
	if err != nil {
		return nil, "", err
	}

	return rostersResponse.Result, nextPageToken, nil
}

// Table `cmn_rota_member` (On-Call Roster Members).
func (c *Client) GetRotaMembers(ctx context.Context, rosterId string, memberId string, paginationVars PaginationVars) ([]RotaMember, string, error) {
	var rotaMembersResponse RotaMembersResponse

	reqOpts := filterToReqOptions(prepareRotaMemberFilter(rosterId, memberId))
	reqOpts = append(reqOpts, paginationVarsToReqOptions(&paginationVars)...)

	nextPageToken, err := c.get(
		ctx,
		c.apiURL(RotaMembersBaseUrl, c.deployment),
		&rotaMembersResponse,
		reqOpts...,
	)
	if err != nil {
		return nil, "", err
	}

	return rotaMembersResponse.Result, nextPageToken, nil
}

// WhoIsOnCall returns the on-call lineup for a roster as of now, via the
// On-Call REST API. The result is ordered (Order==1 is the user currently
// on call; higher orders are the escalation chain). Note: roster members
// must also belong to the underlying assignment group for the on-call
// engine to include them.
func (c *Client) WhoIsOnCall(ctx context.Context, rosterId string) ([]OnCallMember, error) {
	var resp WhoIsOnCallResponse

	_, err := c.get(
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
	_, err := c.get(
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
	_, err := c.get(
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
// table. The on-call engine processes the record and creates the underlying
// cmn_rota_member row (setting the read-only roster field). The user must
// already belong to the rota's assignment group.
func (c *Client) AddOnCallMember(ctx context.Context, payload OnCallAddMemberPayload) error {
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
func (c *Client) RemoveOnCallMember(ctx context.Context, payload OnCallRemoveMemberPayload) error {
	return c.post(
		ctx,
		c.apiURL(OnCallRemoveMemberUrl, c.deployment),
		nil,
		&payload,
		WithIncludeResponseBody(),
	)
}

// Table `sys_user_role` (Roles).
func (c *Client) GetRoles(ctx context.Context, paginationVars PaginationVars) ([]Role, string, error) {
	var rolesResponse RolesResponse

	paginationVars.Limit++
	reqOpts := filterToReqOptions(prepareRoleFilters())
	reqOpts = append(reqOpts, paginationVarsToReqOptions(&paginationVars)...)

	nextPageToken, err := c.get(
		ctx,
		c.apiURL(RolesBaseUrl, c.deployment),
		&rolesResponse,
		reqOpts...,
	)

	if err != nil {
		return nil, "", err
	}

	return rolesResponse.Result, nextPageToken, nil
}

// Table `sys_user_has_role` (User to Role).
func (c *Client) GetUserToRole(ctx context.Context, userId string, roleId string, paginationVars PaginationVars) ([]UserToRole, string, error) {
	var userToRoleResponse UserToRoleResponse

	reqOpts := filterToReqOptions(prepareUserToRoleFilter(userId, roleId))
	reqOpts = append(reqOpts, paginationVarsToReqOptions(&paginationVars)...)

	nextPageToken, err := c.get(
		ctx,
		c.apiURL(UserRolesBaseUrl, c.deployment),
		&userToRoleResponse,
		reqOpts...,
	)

	if err != nil {
		return nil, "", err
	}

	return userToRoleResponse.Result, nextPageToken, nil
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

// Table `sys_group_has_role` (Group to Role).
func (c *Client) GetGroupToRole(ctx context.Context, groupId string, roleId string, paginationVars PaginationVars) ([]GroupToRole, string, error) {
	var groupToRoleResponse GroupToRoleResponse

	reqOpts := filterToReqOptions(prepareGroupToRoleFilter(groupId, roleId))
	reqOpts = append(reqOpts, paginationVarsToReqOptions(&paginationVars)...)
	nextPageToken, err := c.get(
		ctx,
		c.apiURL(GroupRolesBaseUrl, c.deployment),
		&groupToRoleResponse,
		reqOpts...,
	)

	if err != nil {
		return nil, "", err
	}

	return groupToRoleResponse.Result, nextPageToken, nil
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

func (c *Client) get(ctx context.Context, urlAddress string, resourceResponse interface{}, reqOptions ...ReqOpt) (string, error) {
	return c.doRequestWithRetry(
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
	_, err := c.doRequestWithRetry(
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
	_, err := c.doRequestWithRetry(
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
	_, err := c.doRequestWithRetry(
		ctx,
		urlAddress,
		http.MethodDelete,
		nil,
		&resourceResponse,
		reqOptions...,
	)

	return err
}

// IsInvalidTableError reports whether err is a ServiceNow "Invalid table" error.
// ServiceNow returns HTTP 400 with body message "Invalid table <name>" when a
// queried table does not exist on the instance — e.g. the on-call scheduling
// tables (cmn_rota_roster, cmn_rota_member) when the On-Call Scheduling plugin
// (com.snc.on_call_rotation) is not installed. The connector uses this to skip
// optional-module resources gracefully instead of failing the whole sync. The
// error message carries the raw response body (see the >=300 path in
// doRequestWithRetry), so a substring match is reliable.
func IsInvalidTableError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "Invalid table")
}

func handleStatusCode(statusCode int) codes.Code {
	switch statusCode {
	case http.StatusRequestTimeout:
		return codes.DeadlineExceeded
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return codes.Unavailable
	case http.StatusNotFound:
		return codes.NotFound
	case http.StatusUnauthorized:
		return codes.Unauthenticated
	case http.StatusForbidden:
		return codes.PermissionDenied
	case http.StatusConflict:
		return codes.AlreadyExists
	case http.StatusNotImplemented:
		return codes.Unimplemented
	}

	if statusCode >= 500 && statusCode <= 599 {
		return codes.Unavailable
	}

	if statusCode < 200 || statusCode >= 300 {
		return codes.Unknown
	}

	return codes.OK
}

func (c *Client) doRequest(ctx context.Context, urlAddress string, method string, data any, resourceResponse any, reqOptions ...ReqOpt) (string, error) {
	var body io.Reader

	if data != nil {
		jsonBody, err := json.Marshal(data)
		if err != nil {
			return "", err
		}

		body = bytes.NewBuffer(jsonBody)
	}

	req, err := http.NewRequestWithContext(ctx, method, urlAddress, body)
	if err != nil {
		return "", err
	}

	// Set default value
	WithQueryParam("sysparm_exclude_reference_link", "true")(req)

	req.Header.Set("Authorization", c.auth)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if method == http.MethodPost || method == http.MethodPatch {
		req.Header.Set("X-no-response-body", "true")
	}

	for _, o := range reqOptions {
		o(req)
	}

	req.URL.RawQuery = req.URL.Query().Encode()

	rawResponse, err := c.httpClient.Do(req)
	if rawResponse != nil && rawResponse.Body != nil {
		defer rawResponse.Body.Close()
	}
	if err != nil {
		return "", err
	}

	if rawResponse.StatusCode < 0 || rawResponse.StatusCode > math.MaxUint32 {
		return "", errors.New("status code is out of range for uint32")
	}

	if rawResponse.StatusCode >= 300 {
		respBody, _ := io.ReadAll(rawResponse.Body)
		return "", status.Errorf(handleStatusCode(rawResponse.StatusCode), "baton-servicenow: request failed with status %d: %s", rawResponse.StatusCode, string(respBody))
	}

	if method != http.MethodDelete {
		if err := json.NewDecoder(rawResponse.Body).Decode(&resourceResponse); err != nil {
			return "", err
		}
	}

	totalCountHeader := rawResponse.Header.Get("X-Total-Count")
	totalCount, err := ConvertPageToken(totalCountHeader)
	if err != nil {
		return "", err
	}

	var pageToken string
	pagingLinks := linkheader.Parse(rawResponse.Header.Get("Link"))
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
func (c *Client) doRequestWithRetry(ctx context.Context, urlAddress string, method string, data any, resourceResponse any, reqOptions ...ReqOpt) (string, error) {
	l := ctxzap.Extract(ctx)

	var lastErr error
	for attempt := 1; attempt <= maxAuthRetries+1; attempt++ {
		if attempt > 1 {
			delay := time.Duration(attempt-1) * authRetryBaseWait
			l.Debug("baton-servicenow: retrying request after 401",
				zap.String("url", urlAddress),
				zap.String("method", method),
				zap.Int("attempt", attempt),
				zap.Int("max_attempts", maxAuthRetries+1),
				zap.Duration("delay", delay),
			)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}

		pageToken, err := c.doRequest(ctx, urlAddress, method, data, resourceResponse, reqOptions...)
		if err == nil {
			if attempt > 1 {
				l.Debug("baton-servicenow: request succeeded after retry",
					zap.String("url", urlAddress),
					zap.String("method", method),
					zap.Int("attempt", attempt),
				)
			}
			return pageToken, nil
		}

		if status.Code(err) != codes.Unauthenticated {
			return "", err
		}

		l.Debug("baton-servicenow: received 401 unauthorized",
			zap.String("url", urlAddress),
			zap.String("method", method),
			zap.Int("attempt", attempt),
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
	return "", lastErr
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

	next, err := c.get(ctx, c.apiURL(VariableSetM2MBaseUrl, c.deployment), &resp, req...)
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

	next, err := c.get(ctx, c.apiURL(ItemOptionNewBaseUrl, c.deployment), &resp, req...)
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

	next, err := c.get(ctx, c.apiURL(QuestionChoiceBaseUrl, c.deployment), &resp, req...)
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

	next, err := c.get(ctx, c.apiURL(ItemOptionNewBaseUrl, c.deployment), &resp, req...)
	if err != nil {
		return nil, "", err
	}
	return resp.Result, next, nil
}
