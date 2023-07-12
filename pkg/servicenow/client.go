package servicenow

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const BaseURL = "https://%s.service-now.com/api"
const TableAPIBaseURL = BaseURL + "/now/table"

const (
	UsersBaseUrl       = TableAPIBaseURL + "/sys_user"
	UserBaseUrl        = UsersBaseUrl + "/%s"
	GroupsBaseUrl      = TableAPIBaseURL + "/sys_user_group"
	GroupBaseUrl       = GroupsBaseUrl + "/%s"
	RolesBaseUrl       = TableAPIBaseURL + "/sys_user_role"
	GroupMemberBaseUrl = TableAPIBaseURL + "/sys_user_grmember"
)

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

type QueryParam interface {
	setup(params *url.Values)
}

type PaginationVars struct {
	Limit  int
	Offset int
}

func (pV *PaginationVars) setup(params *url.Values) {
	if pV.Limit != 0 {
		params.Set("sysparm_limit", fmt.Sprintf("%d", pV.Limit))
	}

	if pV.Offset != 0 {
		params.Set("sysparm_offset", fmt.Sprintf("%d", pV.Offset))
	}
}

type FilterVars struct {
	Fields []string
	Query  string
}

func (fV *FilterVars) setup(params *url.Values) {
	if len(fV.Fields) != 0 {
		params.Set("sysparm_fields", strings.Join(fV.Fields, ","))
	}

	if fV.Query != "" {
		params.Set("sysparm_query", fV.Query)
	}
}

func prepareUserFilters() *FilterVars {
	return &FilterVars{
		Fields: []string{
			"sys_id", "name", "roles", "user_name", "email", "first_name", "last_name", "active",
		},
	}
}

func prepareRoleFilters() *FilterVars {
	return &FilterVars{
		Fields: []string{
			"sys_id", "grantable", "name",
		},
	}
}

func prepareGroupFilters() *FilterVars {
	return &FilterVars{
		Fields: []string{
			"sys_id", "description", "name",
		},
	}
}

func prepareGroupMemberFilter(groupId string) *FilterVars {
	return &FilterVars{
		Fields: []string{
			"user", "group",
		},
		Query: fmt.Sprintf("group=%s", groupId),
	}
}

func (c *Client) GetUsers(ctx context.Context, paginationVars PaginationVars) ([]User, error) {
	var usersResponse UsersResponse

	err := c.doRequest(
		ctx,
		fmt.Sprintf(UsersBaseUrl, c.deployment),
		&usersResponse,
		[]QueryParam{
			&paginationVars,
			prepareUserFilters(),
		}...,
	)

	if err != nil {
		return nil, err
	}

	return usersResponse.Result, nil
}

func (c *Client) GetUser(ctx context.Context, userId string) (*User, error) {
	var userResponse UserResponse

	err := c.doRequest(
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

func (c *Client) GetGroups(ctx context.Context, paginationVars PaginationVars) ([]Group, error) {
	var groupsResponse GroupsResponse

	err := c.doRequest(
		ctx,
		fmt.Sprintf(GroupsBaseUrl, c.deployment),
		&groupsResponse,
		[]QueryParam{
			&paginationVars,
			prepareGroupFilters(),
		}...,
	)

	if err != nil {
		return nil, err
	}

	return groupsResponse.Result, nil
}

func (c *Client) GetGroup(ctx context.Context, groupId string) (*Group, error) {
	var groupResponse GroupResponse

	err := c.doRequest(
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

func (c *Client) GetGroupMembers(ctx context.Context, groupId string, paginationVars PaginationVars) ([]GroupMember, error) {
	var groupMembersResponse GroupMembersResponse

	err := c.doRequest(
		ctx,
		fmt.Sprintf(GroupMemberBaseUrl, c.deployment),
		&groupMembersResponse,
		[]QueryParam{
			&paginationVars,
			prepareGroupMemberFilter(groupId),
		}...,
	)

	if err != nil {
		return nil, err
	}

	return groupMembersResponse.Result, nil
}

func (c *Client) GetRoles(ctx context.Context, paginationVars PaginationVars) ([]Role, error) {
	var rolesResponse RolesResponse

	err := c.doRequest(
		ctx,
		fmt.Sprintf(RolesBaseUrl, c.deployment),
		&rolesResponse,
		[]QueryParam{
			&paginationVars,
			prepareRoleFilters(),
		}...,
	)

	if err != nil {
		return nil, err
	}

	return rolesResponse.Result, nil
}

func (c *Client) doRequest(
	ctx context.Context,
	urlAddress string,
	resourceResponse interface{},
	paramOptions ...QueryParam,
) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, urlAddress, nil)
	if err != nil {
		return err
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
		return err
	}

	defer rawResponse.Body.Close()

	if rawResponse.StatusCode >= 300 {
		return status.Error(codes.Code(rawResponse.StatusCode), "Request failed")
	}

	if err := json.NewDecoder(rawResponse.Body).Decode(&resourceResponse); err != nil {
		return err
	}

	return nil
}
