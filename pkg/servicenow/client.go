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

	"github.com/tomnomnom/linkheader"
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
type UsersResponse = ListResponse[User]
type RolesResponse = ListResponse[Role]
type GroupsResponse = ListResponse[Group]
type GroupResponse = SingleResponse[Group]
type GroupMembersResponse = ListResponse[GroupMember]
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
	return c.doRequest(
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
	_, err := c.doRequest(
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
	_, err := c.doRequest(
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
	_, err := c.doRequest(
		ctx,
		urlAddress,
		http.MethodDelete,
		nil,
		&resourceResponse,
		reqOptions...,
	)

	return err
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
		return "", status.Errorf(handleStatusCode(rawResponse.StatusCode), "request failed: status code %d", rawResponse.StatusCode)
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
	links, _, err := c.GetVariableSetLinksForItems(ctx, []string{itemSysID}, PaginationVars{Limit: 200})
	if err != nil {
		return nil, fmt.Errorf("failed to get variable set links: %w", err)
	}

	setVars, choicesByQ, err := c.getSetVariablesAndChoices(ctx, links)
	if err != nil {
		return nil, err
	}

	return mergeVariables(itemVars, setVars, choicesByQ), nil
}

// GetCatalogItemVariablesPlusSetsMulti fetches variable set data for multiple catalog items
// in a single batch query instead of per-item. Returns a map of itemSysID -> []CatalogItemVariable.
func (c *Client) GetCatalogItemVariablesPlusSetsMulti(ctx context.Context, itemSysIDs []string) (map[string][]CatalogItemVariable, error) {
	if len(itemSysIDs) == 0 {
		return nil, nil
	}

	// Batch: find all variable set links for all items at once.
	// Use a large limit to avoid truncation. If the result set is at the limit,
	// some links may be missing — this matches the existing per-item behavior
	// which also uses a fixed limit.
	allLinks, _, err := c.GetVariableSetLinksForItems(ctx, itemSysIDs, PaginationVars{Limit: 2000})
	if err != nil {
		return nil, fmt.Errorf("failed to get variable set links: %w", err)
	}

	// Group links by catalog item
	linksByItem := make(map[string][]VariableSetM2M, len(itemSysIDs))
	for _, l := range allLinks {
		linksByItem[l.CatItem] = append(linksByItem[l.CatItem], l)
	}

	// Batch: fetch all set variables and choices at once
	setVars, choicesByQ, err := c.getSetVariablesAndChoices(ctx, allLinks)
	if err != nil {
		return nil, err
	}

	// Index set variables by their variable_set ID for grouping
	varsBySet := make(map[string][]CatalogItemVariable)
	for _, v := range setVars {
		cv := MapItemOptionNewToCatalogItemVariable(v, choicesByQ[v.SysID])
		varsBySet[v.VariableSet] = append(varsBySet[v.VariableSet], cv)
	}

	// Build per-item results
	result := make(map[string][]CatalogItemVariable, len(itemSysIDs))
	for _, itemID := range itemSysIDs {
		var itemSetVars []CatalogItemVariable
		for _, link := range linksByItem[itemID] {
			itemSetVars = append(itemSetVars, varsBySet[link.VariableSet]...)
		}
		result[itemID] = itemSetVars
	}

	return result, nil
}

// getSetVariablesAndChoices fetches variables and their choices for a set of variable set links.
func (c *Client) getSetVariablesAndChoices(ctx context.Context, links []VariableSetM2M) ([]ItemOptionNew, map[string][]QuestionChoice, error) {
	if len(links) == 0 {
		return nil, nil, nil
	}

	setIDs := make([]string, 0, len(links))
	for _, l := range links {
		setIDs = append(setIDs, l.VariableSet)
	}

	// Fetch variables that belong to those sets
	setVars, _, err := c.GetVariablesBySetIDs(ctx, setIDs, PaginationVars{Limit: 500})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get variables by set ids: %w", err)
	}

	// Fetch choices for set variables (so selects have options)
	varIDs := make([]string, 0, len(setVars))
	for _, v := range setVars {
		varIDs = append(varIDs, v.SysID)
	}
	choices, _, err := c.GetChoicesForVariables(ctx, varIDs, PaginationVars{Limit: 1000})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get choices for set variables: %w", err)
	}
	choicesByQ := make(map[string][]QuestionChoice, len(varIDs))
	for _, ch := range choices {
		choicesByQ[ch.Question] = append(choicesByQ[ch.Question], ch)
	}

	return setVars, choicesByQ, nil
}

// mergeVariables merges direct item variables with set variables, preferring direct items on ID collisions.
func mergeVariables(itemVars []CatalogItemVariable, setVars []ItemOptionNew, choicesByQ map[string][]QuestionChoice) []CatalogItemVariable {
	out := make([]CatalogItemVariable, 0, len(itemVars)+len(setVars))
	seen := make(map[string]struct{}, len(itemVars))
	for _, v := range itemVars {
		out = append(out, v)
		seen[v.ID] = struct{}{}
	}
	for _, v := range setVars {
		cv := MapItemOptionNewToCatalogItemVariable(v, choicesByQ[v.SysID])
		if _, dup := seen[cv.ID]; !dup {
			out = append(out, cv)
		}
	}
	return out
}

func (c *Client) GetVariableSetLinksForItem(ctx context.Context, itemSysID string, pg PaginationVars) ([]VariableSetM2M, string, error) {
	return c.GetVariableSetLinksForItems(ctx, []string{itemSysID}, pg)
}

// GetVariableSetLinksForItems fetches variable set links for multiple catalog items in a single query.
func (c *Client) GetVariableSetLinksForItems(ctx context.Context, itemSysIDs []string, pg PaginationVars) ([]VariableSetM2M, string, error) {
	if len(itemSysIDs) == 0 {
		return nil, "", nil
	}
	var query string
	if len(itemSysIDs) == 1 {
		query = fmt.Sprintf("sc_cat_item=%s", itemSysIDs[0])
	} else {
		query = "sc_cat_itemIN" + strings.Join(itemSysIDs, ",")
	}
	var resp VariableSetM2MResponse
	req := []ReqOpt{
		WithQueryParam("sysparm_query", query),
		WithQueryParam("sysparm_fields", "sys_id,variable_set,sc_cat_item"),
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
