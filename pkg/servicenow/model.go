package servicenow

type BaseResource struct {
	Id string `json:"sys_id"`
}

type User struct {
	BaseResource
	Email     string `json:"email"`
	FirstName string `json:"first_name"`
	LastName  string `json:"last_name"`
	UserName  string `json:"user_name"`
	Roles     string `json:"roles"`
}

type Role struct {
	BaseResource
	Name      string `json:"name"`
	Grantable string `json:"grantable"`
}

type Group struct {
	BaseResource
	Name        string `json:"name"`
	Description string `json:"description"`
	Roles       string `json:"roles"`
}

type RecordValue struct {
	Value string `json:"value"`
}

type GroupMember struct {
	BaseResource
	User  RecordValue `json:"user"`
	Group RecordValue `json:"group"`
}

type GroupMemberPayload struct {
	User  string `json:"user"`
	Group string `json:"group"`
}

type UserToRole struct {
	BaseResource
	Inherited string      `json:"inherited"`
	User      RecordValue `json:"user"`
	Role      RecordValue `json:"role"`
}

type UserToRolePayload struct {
	User string `json:"user"`
	Role string `json:"role"`
}

type GroupToRole struct {
	BaseResource
	Inherits string      `json:"inherits"`
	Group    RecordValue `json:"group"`
	Role     RecordValue `json:"role"`
}

type GroupToRolePayload struct {
	Group string `json:"group"`
	Role  string `json:"role"`
}

type UserRoles struct {
	UserName  string   `json:"user_name"`
	FromRole  []string `json:"from_role"`
	FromGroup []string `json:"from_group"`
}
