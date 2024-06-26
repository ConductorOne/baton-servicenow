package servicenow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

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

	// Service Catalogs
	// Catalog requested item
	ServiceCatalogRequestedItemBaseUrl        = TableAPIBaseURL + "/sc_req_item"
	ServiceCatalogRequestedItemDetailsBaseUrl = ServiceCatalogRequestedItemBaseUrl + "/%s"

	// Catalog request
	ServiceCatalogRequestBaseUrl        = TableAPIBaseURL + "/sc_request"
	ServiceCatalogRequestDetailsBaseUrl = ServiceCatalogRequestBaseUrl + "/%s"

	// Catalogs
	ServiceCatalogBaseUrl         = BaseURL + "/sn_sc/servicecatalog"
	ServiceCatalogListCatalogsUrl = ServiceCatalogBaseUrl + "/catalogs"
	ServiceCatalogGetCatalogUrl   = ServiceCatalogListCatalogsUrl + "/%s"

	// Catalog category
	ServiceCatalogCategoryBaseUrl       = ServiceCatalogGetCatalogUrl + "/categories"
	ServiceCatalogCategoryDetailBaseUrl = ServiceCatalogCategoryBaseUrl + "/%s"

	// Catalog items
	ServiceCatalogItemBaseUrl      = ServiceCatalogBaseUrl + "/items"
	ServiceCatalogItemGetUrl       = ServiceCatalogItemBaseUrl + "/%s"
	ServiceCatalogItemVariablesUrl = ServiceCatalogItemGetUrl + "/variables"

	ServiceCatalogAddItemToCartUrl = ServiceCatalogItemGetUrl + "/add_to_cart"
	ServiceCatalogCartSubmitOrder  = ServiceCatalogBaseUrl + "/cart/submit_order"
)

type ListResponse[T any] struct {
	Result []T `json:"result"`
}

type SingleResponse[T any] struct {
	Result T `json:"result"`
}

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
type AddItemToCartResponse = SingleResponse[Cart]
type SubmitCartOrderResponse = SingleResponse[RequestInfo]
type RequestItemResponse = SingleResponse[RequestItem]
type ServiceCatalogRequestResponse = SingleResponse[ServiceCatalogRequest]

type Client struct {
	httpClient *http.Client
	auth       string
	deployment string
}

func NewClient(httpClient *http.Client, auth string, deployment string) *Client {
	return &Client{
		httpClient: httpClient,
		auth:       auth,
		deployment: deployment,
	}
}

// Table `sys_user` (Users).
func (c *Client) GetUsers(ctx context.Context, paginationVars PaginationVars, userIDs []string) ([]User, string, error) {
	var usersResponse UsersResponse

	nextPage, err := c.get(
		ctx,
		fmt.Sprintf(UsersBaseUrl, c.deployment),
		&usersResponse,
		[]QueryParam{
			&paginationVars,
			prepareUserFilters(userIDs),
		}...,
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
		fmt.Sprintf(UserBaseUrl, c.deployment, userId),
		&userResponse,
		[]QueryParam{
			prepareUserFilters(nil),
		}...,
	)

	if err != nil {
		return nil, err
	}

	return &userResponse.Result, nil
}

// Table `sys_user_group` (Groups).
func (c *Client) GetGroups(ctx context.Context, paginationVars PaginationVars, groupIDs []string) ([]Group, string, error) {
	var groupsResponse GroupsResponse

	nextPageToken, err := c.get(
		ctx,
		fmt.Sprintf(GroupsBaseUrl, c.deployment),
		&groupsResponse,
		[]QueryParam{
			&paginationVars,
			prepareGroupFilters(groupIDs),
		}...,
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
		fmt.Sprintf(GroupBaseUrl, c.deployment, groupId),
		&groupResponse,
		[]QueryParam{
			prepareGroupFilters(nil),
		}...,
	)

	if err != nil {
		return nil, err
	}

	return &groupResponse.Result, nil
}

// Table `sys_user_grmember` (Group Members).
func (c *Client) GetUserToGroup(ctx context.Context, userId string, groupId string, paginationVars PaginationVars) ([]GroupMember, string, error) {
	var groupMembersResponse GroupMembersResponse

	nextPageToken, err := c.get(
		ctx,
		fmt.Sprintf(GroupMembersBaseUrl, c.deployment),
		&groupMembersResponse,
		[]QueryParam{
			&paginationVars,
			prepareUserToGroupFilter(userId, groupId),
		}...,
	)

	if err != nil {
		return nil, "", err
	}

	return groupMembersResponse.Result, nextPageToken, nil
}

func (c *Client) AddUserToGroup(ctx context.Context, record GroupMemberPayload) error {
	return c.post(
		ctx,
		fmt.Sprintf(GroupMembersBaseUrl, c.deployment),
		nil,
		&record,
	)
}

func (c *Client) RemoveUserFromGroup(ctx context.Context, id string) error {
	return c.delete(
		ctx,
		fmt.Sprintf(GroupMemberDetailBaseUrl, c.deployment, id),
		nil,
	)
}

// Table `sys_user_role` (Roles).
func (c *Client) GetRoles(ctx context.Context, paginationVars PaginationVars) ([]Role, string, error) {
	var rolesResponse RolesResponse

	paginationVars.Limit++

	nextPageToken, err := c.get(
		ctx,
		fmt.Sprintf(RolesBaseUrl, c.deployment),
		&rolesResponse,
		[]QueryParam{
			&paginationVars,
			prepareRoleFilters(),
		}...,
	)

	if err != nil {
		return nil, "", err
	}

	return rolesResponse.Result, nextPageToken, nil
}

// Table `sys_user_has_role` (User to Role).
func (c *Client) GetUserToRole(ctx context.Context, userId string, roleId string, paginationVars PaginationVars) ([]UserToRole, string, error) {
	var userToRoleResponse UserToRoleResponse

	nextPageToken, err := c.get(
		ctx,
		fmt.Sprintf(UserRolesBaseUrl, c.deployment),
		&userToRoleResponse,
		[]QueryParam{
			&paginationVars,
			prepareUserToRoleFilter(userId, roleId),
		}...,
	)

	if err != nil {
		return nil, "", err
	}

	return userToRoleResponse.Result, nextPageToken, nil
}

func (c *Client) GrantRoleToUser(ctx context.Context, record UserToRolePayload) error {
	return c.post(
		ctx,
		fmt.Sprintf(UserRolesBaseUrl, c.deployment),
		nil,
		&record,
	)
}

func (c *Client) RevokeRoleFromUser(ctx context.Context, id string) error {
	return c.delete(
		ctx,
		fmt.Sprintf(UserRoleDetailBaseUrl, c.deployment, id),
		nil,
	)
}

// Table `sys_group_has_role` (Group to Role).
func (c *Client) GetGroupToRole(ctx context.Context, groupId string, roleId string, paginationVars PaginationVars) ([]GroupToRole, string, error) {
	var groupToRoleResponse GroupToRoleResponse

	nextPageToken, err := c.get(
		ctx,
		fmt.Sprintf(GroupRolesBaseUrl, c.deployment),
		&groupToRoleResponse,
		[]QueryParam{
			&paginationVars,
			prepareGroupToRoleFilter(groupId, roleId),
		}...,
	)

	if err != nil {
		return nil, "", err
	}

	return groupToRoleResponse.Result, nextPageToken, nil
}

func (c *Client) GrantRoleToGroup(ctx context.Context, record GroupToRolePayload) error {
	return c.post(
		ctx,
		fmt.Sprintf(GroupRolesBaseUrl, c.deployment),
		nil,
		&record,
	)
}

func (c *Client) RevokeRoleFromGroup(ctx context.Context, id string) error {
	return c.delete(
		ctx,
		fmt.Sprintf(GroupRoleDetailBaseUrl, c.deployment, id),
		nil,
	)
}

func (c *Client) get(ctx context.Context, urlAddress string, resourceResponse interface{}, paramOptions ...QueryParam) (string, error) {
	return c.doRequest(
		ctx,
		urlAddress,
		http.MethodGet,
		nil,
		&resourceResponse,
		paramOptions...,
	)
}

func (c *Client) post(
	ctx context.Context,
	urlAddress string,
	resourceResponse interface{},
	data interface{},
	paramOptions ...QueryParam,
) error {
	_, err := c.doRequest(
		ctx,
		urlAddress,
		http.MethodPost,
		data,
		&resourceResponse,
		paramOptions...,
	)

	return err
}

func (c *Client) delete(
	ctx context.Context,
	urlAddress string,
	resourceResponse interface{},
	paramOptions ...QueryParam,
) error {
	_, err := c.doRequest(
		ctx,
		urlAddress,
		http.MethodDelete,
		nil,
		&resourceResponse,
		paramOptions...,
	)

	return err
}

func (c *Client) doRequest(ctx context.Context, urlAddress string, method string, data interface{}, resourceResponse interface{}, paramOptions ...QueryParam) (string, error) {
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

	queryParams := url.Values{}
	for _, queryParam := range paramOptions {
		queryParam.setup(&queryParams)
	}

	req.URL.RawQuery = queryParams.Encode()

	req.Header.Set("Authorization", c.auth)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	if method == http.MethodPost {
		req.Header.Set("X-no-response-body", "true")
	}

	rawResponse, err := c.httpClient.Do(req)
	if err != nil {
		return "", err
	}
	defer rawResponse.Body.Close()

	if rawResponse.StatusCode >= 300 {
		return "", status.Error(codes.Code(rawResponse.StatusCode), "Request failed")
	}

	if method != http.MethodDelete {
		if err := json.NewDecoder(rawResponse.Body).Decode(&resourceResponse); err != nil {
			return "", err
		}
	}

	var pageToken string
	pagingLinks := linkheader.Parse(rawResponse.Header.Get("Link"))
	for _, link := range pagingLinks {
		if link.Rel == "next" {
			nextPageUrl, err := url.Parse(link.URL)
			if err != nil {
				return "", err
			}

			pageToken = nextPageUrl.Query().Get("sysparm_offset")
			break
		}
	}

	return pageToken, nil
}
