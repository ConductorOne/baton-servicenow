package servicenow

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	BaseURL          = "https://%s.service-now.com/api"
	TableAPIBaseURL  = BaseURL + "/now/table"
	GlobalApiBaseURL = BaseURL + "/global"

	UsersBaseUrl             = TableAPIBaseURL + "/sys_user"
	UserBaseUrl              = UsersBaseUrl + "/%s"
	GroupsBaseUrl            = TableAPIBaseURL + "/sys_user_group"
	GroupBaseUrl             = GroupsBaseUrl + "/%s"
	RolesBaseUrl             = TableAPIBaseURL + "/sys_user_role"
	GroupMembersBaseUrl      = TableAPIBaseURL + "/sys_user_grmember"
	GroupMemberDetailBaseUrl = TableAPIBaseURL + "/sys_user_grmember/%s"
	UserRolesBaseUrl         = TableAPIBaseURL + "/sys_user_has_role"
	UserRoleDetailBaseUrl    = TableAPIBaseURL + "/sys_user_has_role/%s"
	GroupRolesBaseUrl        = TableAPIBaseURL + "/sys_group_has_role"
	GroupRoleDetailBaseUrl   = TableAPIBaseURL + "/sys_group_has_role/%s"

	UserRoleInheritanceBaseUrl = GlobalApiBaseURL + "/user_role_inheritance"
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
func (c *Client) GetUsers(ctx context.Context, paginationVars PaginationVars) ([]User, int, error) {
	var usersResponse UsersResponse

	total, err := c.get(
		ctx,
		fmt.Sprintf(UsersBaseUrl, c.deployment),
		&usersResponse,
		[]QueryParam{
			&paginationVars,
			prepareUserFilters(),
		}...,
	)

	if err != nil {
		return nil, total, err
	}

	return usersResponse.Result, total, nil
}

func (c *Client) GetUser(ctx context.Context, userId string) (*User, error) {
	var userResponse UserResponse

	_, err := c.get(
		ctx,
		fmt.Sprintf(UserBaseUrl, c.deployment, userId),
		&userResponse,
		[]QueryParam{
			prepareUserFilters(),
		}...,
	)

	if err != nil {
		return nil, err
	}

	return &userResponse.Result, nil
}

// Table `sys_user_group` (Groups).
func (c *Client) GetGroups(ctx context.Context, paginationVars PaginationVars) ([]Group, int, error) {
	var groupsResponse GroupsResponse

	total, err := c.get(
		ctx,
		fmt.Sprintf(GroupsBaseUrl, c.deployment),
		&groupsResponse,
		[]QueryParam{
			&paginationVars,
			prepareGroupFilters(),
		}...,
	)

	if err != nil {
		return nil, total, err
	}

	return groupsResponse.Result, total, nil
}

func (c *Client) GetGroup(ctx context.Context, groupId string) (*Group, error) {
	var groupResponse GroupResponse

	_, err := c.get(
		ctx,
		fmt.Sprintf(GroupBaseUrl, c.deployment, groupId),
		&groupResponse,
		[]QueryParam{
			prepareGroupFilters(),
		}...,
	)

	if err != nil {
		return nil, err
	}

	return &groupResponse.Result, nil
}

// Table `sys_user_grmember` (Group Members).
func (c *Client) GetUserToGroup(ctx context.Context, userId string, groupId string, paginationVars PaginationVars) ([]GroupMember, int, error) {
	var groupMembersResponse GroupMembersResponse

	total, err := c.get(
		ctx,
		fmt.Sprintf(GroupMembersBaseUrl, c.deployment),
		&groupMembersResponse,
		[]QueryParam{
			&paginationVars,
			prepareUserToGroupFilter(userId, groupId),
		}...,
	)

	if err != nil {
		return nil, total, err
	}

	return groupMembersResponse.Result, total, nil
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
func (c *Client) GetRoles(ctx context.Context, paginationVars PaginationVars) ([]Role, int, error) {
	var rolesResponse RolesResponse

	paginationVars.Limit++

	total, err := c.get(
		ctx,
		fmt.Sprintf(RolesBaseUrl, c.deployment),
		&rolesResponse,
		[]QueryParam{
			&paginationVars,
			prepareRoleFilters(),
		}...,
	)

	if err != nil {
		return nil, total, err
	}

	return rolesResponse.Result, total, nil
}

// Table `sys_user_has_role` (User to Role).
func (c *Client) GetUserToRole(ctx context.Context, userId string, roleId string, paginationVars PaginationVars) ([]UserToRole, int, error) {
	var userToRoleResponse UserToRoleResponse

	total, err := c.get(
		ctx,
		fmt.Sprintf(UserRolesBaseUrl, c.deployment),
		&userToRoleResponse,
		[]QueryParam{
			&paginationVars,
			prepareUserToRoleFilter(userId, roleId),
		}...,
	)

	if err != nil {
		return nil, total, err
	}

	return userToRoleResponse.Result, total, nil
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
		nil,
	)
}

// Table `sys_group_has_role` (Group to Role).
func (c *Client) GetGroupToRole(ctx context.Context, groupId string, roleId string, paginationVars PaginationVars) ([]GroupToRole, int, error) {
	var groupToRoleResponse GroupToRoleResponse

	total, err := c.get(
		ctx,
		fmt.Sprintf(GroupRolesBaseUrl, c.deployment),
		&groupToRoleResponse,
		[]QueryParam{
			&paginationVars,
			prepareGroupToRoleFilter(groupId, roleId),
		}...,
	)

	if err != nil {
		return nil, total, err
	}

	return groupToRoleResponse.Result, total, nil
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
		nil,
	)
}

// User Role Inheritance API containing roles attached to a user
// TODO: decide to remove this or not
func (c *Client) GetUserRoles(ctx context.Context, userId string) (*UserRoles, int, error) {
	var userRolesResponse UserRolesResponse

	total, err := c.get(
		ctx,
		fmt.Sprintf(UserRoleInheritanceBaseUrl, c.deployment),
		&userRolesResponse,
		[]QueryParam{
			&FilterVars{
				UserId: userId,
			},
		}...,
	)

	if err != nil {
		return nil, total, err
	}

	userRoles := UserRoles{
		FromRole:  []string{},
		FromGroup: []string{},
		UserName:  userRolesResponse.Result.UserName,
	}

	// verbose flag in later for this
	for _, role := range userRolesResponse.Result.FromRole {
		after, _ := strings.CutPrefix(role, "/")
		if strings.Count(after, "/") == 0 {
			userRoles.FromRole = append(userRoles.FromRole, role)
		}
	}

	for _, role := range userRolesResponse.Result.FromGroup {
		after, _ := strings.CutPrefix(role, "/")
		if strings.Count(after, "/") == 0 {
			userRoles.FromGroup = append(userRoles.FromGroup, role)
		}
	}

	return &userRoles, total, nil
}

func (c *Client) get(
	ctx context.Context,
	urlAddress string,
	resourceResponse interface{},
	paramOptions ...QueryParam,
) (int, error) {
	return c.doRequest(
		ctx,
		urlAddress,
		http.MethodGet,
		nil,
		&resourceResponse,
		paramOptions...,
	)
}

// TODO: implement `X-no-response-body` header to avoid parsing the response body
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

// TODO: implement `X-no-response-body` header to avoid parsing the response body
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

// TODO: implement annotations for X-Total-Count header
func (c *Client) doRequest(
	ctx context.Context,
	urlAddress string,
	method string,
	data interface{},
	resourceResponse interface{},
	paramOptions ...QueryParam,
) (int, error) {
	var body io.Reader

	if data != nil {
		jsonBody, err := json.Marshal(data)
		if err != nil {
			return 0, err
		}

		body = bytes.NewBuffer(jsonBody)
	}

	req, err := http.NewRequestWithContext(ctx, method, urlAddress, body)
	if err != nil {
		return 0, err
	}

	queryParams := url.Values{}
	for _, queryParam := range paramOptions {
		queryParam.setup(&queryParams)
	}

	req.URL.RawQuery = queryParams.Encode()

	req.Header.Set("Authorization", c.auth)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	rawResponse, err := c.httpClient.Do(req)
	if err != nil {
		return 0, err
	}

	defer rawResponse.Body.Close()

	if rawResponse.StatusCode >= 300 {
		return 0, status.Error(codes.Code(rawResponse.StatusCode), "Request failed")
	}

	if err := json.NewDecoder(rawResponse.Body).Decode(&resourceResponse); err != nil {
		return 0, err
	}

	// extract header X-Total-Count and return it
	xTotalCount := rawResponse.Header.Get("X-Total-Count")
	if xTotalCount != "" {
		total, err := strconv.Atoi(xTotalCount)
		if err != nil {
			return 0, err
		}

		return total, nil
	}

	return 0, nil
}
