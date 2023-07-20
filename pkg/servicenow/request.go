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

func prepareUserFilters() *FilterVars {
	return &FilterVars{
		Fields: UserFields,
	}
}

func prepareUsersFilters(ids []string) *FilterVars {
	return &FilterVars{
		Fields: UserFields,
		Query:  strings.Join(ids, "^OR"),
	}
}

func prepareRoleFilters() *FilterVars {
	return &FilterVars{
		Fields: RoleFields,
		Query:  "grantable=true",
	}
}

func prepareGroupFilters() *FilterVars {
	return &FilterVars{
		Fields: GroupFields,
	}
}

func prepareGroupsFilters(ids []string) *FilterVars {
	return &FilterVars{
		Fields: GroupFields,
		Query:  strings.Join(ids, "^OR"),
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
