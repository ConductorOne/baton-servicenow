package servicenow

import (
	"fmt"
	"net/url"
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
	UserId string
}

func (fV *FilterVars) setup(params *url.Values) {
	if len(fV.Fields) != 0 {
		params.Set("sysparm_fields", strings.Join(fV.Fields, ","))
	}

	if fV.Query != "" {
		params.Set("sysparm_query", fV.Query)
	}

	if fV.UserId != "" {
		params.Set("user_sysid", fV.UserId)
	}
}

func prepareUserFilters(ids []string) *FilterVars {
	var query string

	if ids != nil {
		query = queryMultipleIDs(ids)
	}

	return &FilterVars{
		Fields: UserFields,
		Query:  query,
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

func prepareUserToGroupFilter(userId string, groupId string) *FilterVars {
	var query string

	if userId != "" {
		query = fmt.Sprintf("user=%s", userId)
	}

	if groupId != "" {
		if query != "" {
			query = fmt.Sprintf("%s^group=%s", query, groupId)
		} else {
			query = fmt.Sprintf("group=%s", groupId)
		}
	}

	return &FilterVars{
		Fields: []string{
			"sys_id", "user", "group",
		},
		Query: query,
	}
}

func prepareUserToRoleFilter(userId string, roleId string) *FilterVars {
	var query string
	if userId != "" {
		query = fmt.Sprintf("user=%s", userId)
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
			"sys_id", "user", "role", "inherited",
		},
		Query: query,
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

func prepareServiceCatalogFilters() *FilterVars {
	return &FilterVars{}
}
