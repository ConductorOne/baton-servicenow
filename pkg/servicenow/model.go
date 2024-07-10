package servicenow

import (
	"context"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	sdkTicket "github.com/conductorone/baton-sdk/pkg/types/ticket"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
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
	BaseResource
	Catalogs         []Catalog             `json:"catalogs"`
	Category         Category              `json:"category,omitempty"`
	Description      string                `json:"description"`
	Name             string                `json:"name"`
	ShortDescription string                `json:"short_description"`
	SysClassName     string                `json:"sys_class_name"`
	Type             string                `json:"type"`
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
	Approval            string           `json:"approval"`

	ShortDescription     string `json:"short_description"`
	Description          string `json:"description"`
	CloseNotes           string `json:"close_notes"`
	AssignedTo           string `json:"assigned_to"`
	Comments             string `json:"comments"`
	CommentsAndWorkNotes string `json:"comments_and_work_notes"`
	UponApproval         string `json:"upon_approval"`
}

type RequestedItem struct {
	BaseResource

	State               string `json:"state"`
	Description         string `json:"description"`
	Number              string `json:"number"`
	TaskEffectiveNumber string `json:"task_effective_number"`

	Request  ResourceRefLink `json:"request"`
	CatItem  ResourceRefLink `json:"cat_item"`
	Catalogs []Catalog       `json:"catalogs,omitempty"`
	Category Category        `json:"category,omitempty"`

	ScCatalog        string           `json:"sc_catalog,omitempty"`
	RequestedFor     *ResourceRefLink `json:"requested_for,omitempty"`
	SysUpdatedOn     string           `json:"sys_updated_on"`
	SysUpdatedBy     string           `json:"sys_updated_by"`
	ClosedAt         string           `json:"closed_at"`
	OpenedBy         ResourceRefLink  `json:"opened_by"`
	SysCreatedOn     string           `json:"sys_created_on"`
	SysCreatedBy     string           `json:"sys_created_by"`
	Active           string           `json:"active"`
	OpenedAt         string           `json:"opened_at"`
	ShortDescription string           `json:"short_description"`
	Approval         string           `json:"approval"`
	AssignedTo       string           `json:"assigned_to"`
}

type Choice struct {
	Index int    `json:"index"`
	Label string `json:"label"`
	Value string `json:"value"`
}

type CatalogItemVariable struct {
	Active                  bool            `json:"active"`
	Label                   string          `json:"label"`
	DynamicValueField       string          `json:"dynamic_value_field"`
	Type                    VariableTypeNew `json:"type"`
	Mandatory               bool            `json:"mandatory"`
	DisplayValue            string          `json:"displayvalue"`
	FriendlyType            string          `json:"friendly_type"`
	DisplayType             string          `json:"display_type"`
	RenderLabel             bool            `json:"render_label"`
	ReadOnly                bool            `json:"read_only"`
	Name                    string          `json:"name"`
	Attributes              string          `json:"attributes"`
	ID                      string          `json:"id"`
	Choices                 []Choice        `json:"choices"`
	Value                   string          `json:"value"`
	DynamicValueDotWalkPath string          `json:"dynamic_value_dot_walk_path"`
	HelpText                string          `json:"help_text"`
	Order                   int             `json:"order"`
	Reference               string          `json:"reference"`
	RefQualifier            string          `json:"ref_qualifier"`
}

type OrderItemPayload struct {
	Quantity     int                    `json:"sysparm_quantity"`
	RequestedFor string                 `json:"sysparm_requested_for"`
	Variables    map[string]interface{} `json:"variables"`
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

type VariableTypeNew interface{}

type VariableType int

// TODO(lauren) not sure how to handle all of these
// These correspond to variable type id
const (
	UNSPECIFIED VariableType = iota
	YES_NO
	MULTI_LINE_TEXT
	MULTIPLE_CHOICE
	NUMERIC_SCALE // TODO(lauren) add baton custom field type for number
	SELECT_BOX
	SINGLE_LINE_TEXT
	CHECK_BOX
	REFERENCE
	DATE
	DATE_TIME
	LABEL
	BREAK
	_
	MACRO   // skip
	UI_PAGE //skip
	WIDE_SINGLE_LINE_TEXT
	MACRO_WITH_LABEL //skip
	LOOKUP_SELECT_BOX
	CONTAINER_START
	CONTAINER_END
	LIST_COLLECTOR
	LOOKUP_MULTIPLE_CHOICE
	HTML
	SPLIT
	MASKED
	EMAIL
	URL
	IP_ADDRESS
	DURATION
	_
	REQUESTED_FOR
	RICH_TEXT_LABEL
	ATTACHMENT
)

// TODO(lauren) add validation?
func ConvertVariableToSchemaCustomField(ctx context.Context, variable *CatalogItemVariable) (*v2.TicketCustomField, error) {
	if !variable.Active || variable.ReadOnly {
		return nil, nil
	}

	// TODO(unmarshal func)
	var typ VariableType
	t, ok := variable.Type.(float64)
	if !ok {
		typ = UNSPECIFIED
	} else {
		typ = VariableType(int(t))
	}

	switch typ {
	case UNSPECIFIED:
		return nil, nil
	case YES_NO, CHECK_BOX:
		return sdkTicket.BoolFieldSchema(variable.Name, variable.Name, variable.Mandatory), nil
	case MULTI_LINE_TEXT, SINGLE_LINE_TEXT, WIDE_SINGLE_LINE_TEXT:
		return sdkTicket.StringFieldSchema(variable.Name, variable.Name, variable.Mandatory), nil
	case MULTIPLE_CHOICE, LOOKUP_MULTIPLE_CHOICE:
		var allowedChoices []*v2.TicketCustomFieldObjectValue
		choices := variable.Choices
		for _, c := range choices {
			allowedChoices = append(allowedChoices, &v2.TicketCustomFieldObjectValue{
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
				Id:          c.Value,
				DisplayName: c.Value,
			})
		}
		return sdkTicket.PickObjectValueFieldSchema(variable.Name, variable.Name, variable.Mandatory, allowedChoices), nil
	case HTML:
		return sdkTicket.StringFieldSchema(variable.Name, variable.Name, variable.Mandatory), nil
	case REFERENCE:
		// This should be a sys_id
		return sdkTicket.StringFieldSchema(variable.Name, variable.Name, variable.Mandatory), nil
	case EMAIL:
		return sdkTicket.StringFieldSchema(variable.Name, variable.Name, variable.Mandatory), nil
	case URL:
		return sdkTicket.StringFieldSchema(variable.Name, variable.Name, variable.Mandatory), nil
	case IP_ADDRESS:
		return sdkTicket.StringFieldSchema(variable.Name, variable.Name, variable.Mandatory), nil
	case REQUESTED_FOR:
		// This should be sys_id of user
		return sdkTicket.StringFieldSchema(variable.Name, variable.Name, variable.Mandatory), nil
	case LIST_COLLECTOR: // TODO(lauren) I think this just takes sys_ids but in the UI its populated from other tables
		return nil, nil
	case DURATION: // TODO(lauren) make duration field?
		return sdkTicket.StringFieldSchema(variable.Name, variable.Name, variable.Mandatory), nil
	default:
		// TODO(lauren) should continue instead of erroring?
		if variable.Mandatory {
			l := ctxzap.Extract(ctx)
			l.Error("unsupported mandatory type", zap.Any("var", variable))
			return nil, nil
			//return nil, errors.New("unsupported mandatory type")
		}
		return nil, nil
	}
}
