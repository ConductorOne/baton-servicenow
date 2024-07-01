package servicenow

import (
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	sdkTicket "github.com/conductorone/baton-sdk/pkg/types/ticket"
)

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
	BaseResource
	User  string `json:"user"`
	Group string `json:"group"`
}

type GroupMemberPayload struct {
	User  string `json:"user"`
	Group string `json:"group"`
}

type UserToRole struct {
	BaseResource
	Inherited string `json:"inherited"`
	User      string `json:"user"`
	Role      string `json:"role"`
}

type UserToRolePayload struct {
	User string `json:"user"`
	Role string `json:"role"`
}

type GroupToRole struct {
	BaseResource
	Inherits string `json:"inherits"`
	Group    string `json:"group"`
	Role     string `json:"role"`
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

// TODO(lauren) remove unecessary fields
// Service Catalog request models
type ResourceRefLink struct {
	Link  string `json:"link"`
	Value string `json:"value"`
}

type Catalog struct {
	BaseResource
	Title         string `json:"title"`
	HasCategories bool   `json:"has_categories,omitempty"`
	HasItems      bool   `json:"has_items,omitempty"`
	Description   string `json:"description,omitempty"`
}

type Category struct {
	BaseResource
	Title           string        `json:"title"`
	Description     string        `json:"description,omitempty"`
	FullDescription string        `json:"full_description,omitempty"`
	ItemCount       int           `json:"count,omitempty"`
	Subcategories   []SubCategory `json:"subcategories,omitempty"`
}

type SubCategory struct {
	SysID string `json:"sys_id"`
	Title string `json:"title"`
}

type CatalogItem struct {
	Catalogs         []Catalog             `json:"catalogs"`
	Category         Category              `json:"category"`
	ContentType      string                `json:"content_type"`
	Description      string                `json:"description"`
	Name             string                `json:"name"`
	Order            int                   `json:"order"`
	ShortDescription string                `json:"short_description"`
	SysClassName     string                `json:"sys_class_name"`
	SysID            string                `json:"sys_id"`
	Type             string                `json:"type"`
	URL              string                `json:"url"`
	Variables        []CatalogItemVariable `json:"variables,omitempty"`
}

type ServiceCatalogRequest struct {
	BaseResource

	Parent string `json:"parent"`

	State               string           `json:"state"`
	RequestState        string           `json:"request_state"`
	Number              string           `json:"number"`
	TaskEffectiveNumber string           `json:"task_effective_number"`
	UponReject          string           `json:"upon_reject"`
	OpenedBy            *ResourceRefLink `json:"opened_by,omitempty"`
	RequestedFor        *ResourceRefLink `json:"requested_for,omitempty"`
	SysCreatedOn        string           `json:"sys_created_on"`
	SysUpdatedOn        string           `json:"sys_updated_on"`
	OpenedAt            string           `json:"opened_at"`
	ClosedAt            string           `json:"closed_at"`
	ApprovalSet         string           `json:"approval_set"`
	SysUpdatedBy        string           `json:"sys_updated_by"`
	SysCreatedBy        string           `json:"sys_created_by"`
	Priority            string           `json:"priority"`
	Approval            string           `json:"approval"`

	ShortDescription       string `json:"short_description"`
	AssignmentGroup        string `json:"assignment_group"`
	AdditionalAssigneeList string `json:"additional_assignee_list"`
	Description            string `json:"description"`
	CloseNotes             string `json:"close_notes"`
	ClosedBy               string `json:"closed_by"`
	RequestedDate          string `json:"requested_date"`
	AssignedTo             string `json:"assigned_to"`
	Comments               string `json:"comments"`
	CommentsAndWorkNotes   string `json:"comments_and_work_notes"`
	DueDate                string `json:"due_date"`
	SysTags                string `json:"sys_tags"`
	UponApproval           string `json:"upon_approval"`
}

type RequestedItem struct {
	BaseResource

	State               string `json:"state"`
	Description         string `json:"description"`
	Number              string `json:"number"`
	TaskEffectiveNumber string `json:"task_effective_number"`
	Stage               string `json:"stage"`

	Request  ResourceRefLink `json:"request"`
	CatItem  ResourceRefLink `json:"cat_item"`
	Catalogs []Catalog       `json:"catalogs"`
	Category Category        `json:"category"`
	SysTags  string          `json:"sys_tags"`

	Parent           string          `json:"parent"`
	ScCatalog        string          `json:"sc_catalog"`
	UponReject       string          `json:"upon_reject"`
	RequestedFor     ResourceRefLink `json:"requested_for"`
	SysUpdatedOn     string          `json:"sys_updated_on"`
	SysUpdatedBy     string          `json:"sys_updated_by"`
	ClosedAt         string          `json:"closed_at"`
	ClosedBy         string          `json:"closed_by"`
	OpenedBy         ResourceRefLink `json:"opened_by"`
	SysCreatedOn     string          `json:"sys_created_on"`
	SysCreatedBy     string          `json:"sys_created_by"`
	Active           string          `json:"active"`
	OpenedAt         string          `json:"opened_at"`
	ShortDescription string          `json:"short_description"`
	Approval         string          `json:"approval"`
	UponApproval     string          `json:"upon_approval"`
	AssignedTo       string          `json:"assigned_to"`
}

type Choice struct {
	Index int    `json:"index"`
	Label string `json:"label"`
	Value string `json:"value"`
}

type CatalogItemVariable struct {
	Active                  bool     `json:"active"`
	Label                   string   `json:"label"`
	DynamicValueField       string   `json:"dynamic_value_field"`
	Type                    int      `json:"type"`
	Mandatory               bool     `json:"mandatory"`
	DisplayValue            string   `json:"displayvalue"`
	FriendlyType            string   `json:"friendly_type"`
	DisplayType             string   `json:"display_type"`
	RenderLabel             bool     `json:"render_label"`
	ReadOnly                bool     `json:"read_only"`
	Name                    string   `json:"name"`
	Attributes              string   `json:"attributes"`
	ID                      string   `json:"id"`
	Choices                 []Choice `json:"choices"`
	Value                   string   `json:"value"`
	DynamicValueDotWalkPath string   `json:"dynamic_value_dot_walk_path"`
	HelpText                string   `json:"help_text"`
	MaxLength               int      `json:"max_length"`
	Order                   int      `json:"order"`
}

type AddItemToCartPayload struct {
	Quantity     int                    `json:"sysparm_quantity"`
	RequestedFor string                 `json:"sysparm_requested_for"`
	Variables    map[string]interface{} `json:"variables"`
}

type CartItem struct {
	CartItemID    string `json:"cart_item_id"`
	CatalogItemID string `json:"catalog_item_id"`
	ItemName      string `json:"item_name"`
}

type Cart struct {
	CartID string     `json:"cart_id"`
	Items  []CartItem `json:"items"`
}

type RequestInfo struct {
	RequestNumber string `json:"request_number"`
	RequestID     string `json:"request_id"`
}

type Label struct {
	Id         string `json:"sys_id,omitempty"`
	Name       string `json:"name"`
	ViewableBy string `json:"viewable_by"`
}

type RequestItemState struct {
	Label string `json:"label"`
	Value string `json:"value"`
}

type LabelEntryPayload struct {
	Table    string `json:"table"`
	Label    string `json:"label"`
	TableKey string `json:"table_key"`
}

type LabelEntryName struct {
	LabelName string `json:"label.name"`
}

type VariableType int

// TODO(lauren) not sure how to handle all of these
// These correspond to variable type id
const (
	UNSPECIFIED = iota
	YES_NO
	MULTI_LINE_TEXT
	MULTIPLE_CHOICE
	NUMERIC_SCALE
	SELECT_BOX
	SINGLE_LINE_TEXT
	CHECK_BOX
	REFERENCE
	DATE
	DATE_TIME
	LABEL
	BREAK
	UNKNOWN // Not sure what 13 is
	MACRO
	UI_PAGE
	WIDE_SINGLE_LINE_TEXT
	MACRO_WITH_LABEL
	LOOKUP_SELECT_BOX
	CONTAINER_START
	CONTAINER_END
	LIST_COLLECTOR
	LOOKUP_MULTIPLE_CHOICE
	HTML
	SPLIT
	MASKED
)

func ConvertVariableToSchemaCustomField(variable *CatalogItemVariable) (*v2.TicketCustomField, error) {
	switch variable.Type {
	case YES_NO, CHECK_BOX:
		return sdkTicket.BoolFieldSchema(variable.Name, variable.Name, variable.Mandatory), nil
	case MULTI_LINE_TEXT, SINGLE_LINE_TEXT, LABEL, WIDE_SINGLE_LINE_TEXT: // Not sure if label should be here
		return sdkTicket.StringFieldSchema(variable.Name, variable.Name, variable.Mandatory), nil
	case MULTIPLE_CHOICE, LOOKUP_MULTIPLE_CHOICE:
		var allowedChoices []*v2.TicketCustomFieldObjectValue
		choices := variable.Choices
		for _, c := range choices {
			allowedChoices = append(allowedChoices, &v2.TicketCustomFieldObjectValue{
				//TODO(lauren) is this ok for ID? Or use c.Label/c.index?
				Id:          c.Value,
				DisplayName: c.Value,
			})
		}
		return sdkTicket.PickMultipleObjectValuesFieldSchema(variable.Name, variable.Name, variable.Mandatory, allowedChoices), nil
	case DATE, DATE_TIME:
		return sdkTicket.TimestampFieldSchema(variable.Name, variable.Name, variable.Mandatory), nil
	case SELECT_BOX, LOOKUP_SELECT_BOX:
		var allowedChoices []*v2.TicketCustomFieldObjectValue
		choices := variable.Choices
		for _, c := range choices {
			allowedChoices = append(allowedChoices, &v2.TicketCustomFieldObjectValue{
				//TODO(lauren) is this ok for ID? Or use c.Label/c.index?
				Id:          c.Value,
				DisplayName: c.Value,
			})
		}
		return sdkTicket.PickObjectValueFieldSchema(variable.Name, variable.Name, variable.Mandatory, allowedChoices), nil
		//return sdkTicket.PickObjectValueFieldSchema(variable.ID, variable.Name, variable.Mandatory, allowedChoices), nil
	default:
		// Have seen REFERENCE (8), MACRO(14) variable types, needs more investigation
		// TODO(lauren) handle this
		/*if variable.Mandatory {
			return nil, errors.New("Unsupported mandatory type")
		}*/
		return nil, nil
	}
}
