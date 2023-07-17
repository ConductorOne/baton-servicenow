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

type GroupMember struct {
	User struct {
		Value string `json:"value"`
	} `json:"user"`
	Group struct {
		Value string `json:"value"`
	} `json:"group"`
}

type UserToRole struct {
	Inherited string `json:"inherited"`
	User      struct {
		Value string `json:"value"`
	} `json:"user"`
	Role struct {
		Value string `json:"value"`
	} `json:"role"`
}

type GroupToRole struct {
	Inherits string `json:"inherits"`
	Group    struct {
		Value string `json:"value"`
	} `json:"group"`
	Role struct {
		Value string `json:"value"`
	} `json:"role"`
}

type UserRoles struct {
	UserName  string   `json:"user_name"`
	FromRole  []string `json:"from_role"`
	FromGroup []string `json:"from_group"`
}
