package servicenow

import (
	"fmt"
	"net/url"
	"strings"
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
		Query: "grantable=true",
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

func prepareRoleUsersFilter(roleId string) *FilterVars {
	return &FilterVars{
		Fields: []string{
			"role", "user", "inherited",
		},
		Query: fmt.Sprintf("role=%s^inherited=false", roleId),
	}
}

func prepareRoleGroupsFilter(roleId string) *FilterVars {
	return &FilterVars{
		Fields: []string{
			"role", "group", "inherits",
		},
		Query: fmt.Sprintf("role=%s^inherits=true", roleId),
	}
}
