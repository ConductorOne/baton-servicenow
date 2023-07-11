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
	RolesBaseUrl       = TableAPIBaseURL + "/sys_user_role"
	GroupBaseUrl       = TableAPIBaseURL + "/sys_user_group"
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

type Response[T any] struct {
	Result []T `json:"result"`
}

type UsersResponse = Response[User]
type RolesResponse = Response[Role]
type GroupsResponse = Response[Group]
type GroupMembersResponse = Response[GroupMember]

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

func PreparePagingVars(limit int, offset int) PaginationVars {
	return PaginationVars{
		Limit:  limit,
		Offset: offset,
	}
}

func PrepareUserFilters() FilterVars {
	return FilterVars{
		Fields: []string{
			"sys_id", "name", "roles", "user_name", "email", "first_name", "last_name", "active",
		},
	}
}

func PrepareRoleFilters() FilterVars {
	return FilterVars{
		Fields: []string{
			"sys_id", "grantable", "name",
		},
	}
}

func PrepareGroupFilters() FilterVars {
	return FilterVars{
		Fields: []string{
			"sys_id", "description", "name",
		},
	}
}

func (c *Client) GetUsers(ctx context.Context, paginationVars PaginationVars, filterVars FilterVars) ([]User, int, error) {
	var usersResponse UsersResponse

	err := c.doRequest(
		ctx,
		fmt.Sprintf(UsersBaseUrl, c.deployment),
		&usersResponse,
		[]QueryParam{
			&paginationVars,
			&filterVars,
		}...,
	)

	if err != nil {
		return nil, 0, err
	}

	return usersResponse.Result, paginationVars.Offset, nil
}

func (c *Client) GetRoles(ctx context.Context, paginationVars PaginationVars, filterVars FilterVars) ([]Role, int, error) {
	var rolesResponse RolesResponse

	err := c.doRequest(
		ctx,
		fmt.Sprintf(RolesBaseUrl, c.deployment),
		&rolesResponse,
		[]QueryParam{
			&paginationVars,
			&filterVars,
		}...,
	)

	if err != nil {
		return nil, 0, err
	}

	return rolesResponse.Result, paginationVars.Offset, nil
}

func (c *Client) GetGroups(ctx context.Context, paginationVars PaginationVars, filterVars FilterVars) ([]Group, int, error) {
	var groupsResponse GroupsResponse

	err := c.doRequest(
		ctx,
		fmt.Sprintf(GroupBaseUrl, c.deployment),
		&groupsResponse,
		[]QueryParam{
			&paginationVars,
			&filterVars,
		}...,
	)

	if err != nil {
		return nil, 0, err
	}

	return groupsResponse.Result, paginationVars.Offset, nil
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
