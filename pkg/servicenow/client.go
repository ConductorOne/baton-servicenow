package servicenow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/tomnomnom/linkheader"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	BaseURL          = "https://%s.service-now.com/api"
	TableAPIBaseURL  = BaseURL + "/now/table"
	GlobalApiBaseURL = BaseURL + "/global"

	UsersBaseUrl = TableAPIBaseURL + "/sys_user"
	UserBaseUrl  = UsersBaseUrl + "/%s"

	GroupsBaseUrl = TableAPIBaseURL + "/sys_user_group"
	GroupBaseUrl  = GroupsBaseUrl + "/%s"

	GroupMembersBaseUrl      = TableAPIBaseURL + "/sys_user_grmember"
	GroupMemberDetailBaseUrl = GroupMembersBaseUrl + "/%s"

	// AuditDeleteBaseUrl is the sys_audit_delete table: one row per hard delete
	// of an audited record. The event feed reads it to emit grant-revoke events.
	AuditDeleteBaseUrl = TableAPIBaseURL + "/sys_audit_delete"

	// AuditBaseUrl is the sys_audit table: one row per field-level change of an
	// audited record (tablename/documentkey/fieldname/oldvalue/newvalue). The
	// event feed reads it to surface account changes (sys_user) and audited
	// deletes (fieldname=="DELETED") on the access tables in near real time.
	AuditBaseUrl = TableAPIBaseURL + "/sys_audit"

	RolesBaseUrl           = TableAPIBaseURL + "/sys_user_role"
	UserRolesBaseUrl       = TableAPIBaseURL + "/sys_user_has_role"
	UserRoleDetailBaseUrl  = UserRolesBaseUrl + "/%s"
	GroupRolesBaseUrl      = TableAPIBaseURL + "/sys_group_has_role"
	GroupRoleDetailBaseUrl = GroupRolesBaseUrl + "/%s"

	UserRoleInheritanceBaseUrl = GlobalApiBaseURL + "/user_role_inheritance"

	// Service Catalogs.
	ServiceCatalogRequestedItemBaseUrl        = TableAPIBaseURL + "/sc_req_item"
	ServiceCatalogRequestedItemDetailsBaseUrl = ServiceCatalogRequestedItemBaseUrl + "/%s"

	ServiceCatalogRequestBaseUrl        = TableAPIBaseURL + "/sc_request"
	ServiceCatalogRequestDetailsBaseUrl = ServiceCatalogRequestBaseUrl + "/%s"

	ServiceCatalogBaseUrl         = BaseURL + "/sn_sc/servicecatalog"
	ServiceCatalogListCatalogsUrl = ServiceCatalogBaseUrl + "/catalogs"
	ServiceCatalogGetCatalogUrl   = ServiceCatalogListCatalogsUrl + "/%s"

	ServiceCatalogCategoryBaseUrl       = ServiceCatalogGetCatalogUrl + "/categories"
	ServiceCatalogCategoryDetailBaseUrl = ServiceCatalogCategoryBaseUrl + "/%s"

	ServiceCatalogItemBaseUrl      = ServiceCatalogBaseUrl + "/items"
	ServiceCatalogItemGetUrl       = ServiceCatalogItemBaseUrl + "/%s"
	ServiceCatalogItemVariablesUrl = ServiceCatalogItemGetUrl + "/variables"

	ServiceCatalogOrderItemUrl = ServiceCatalogItemGetUrl + "/order_now"

	LabelBaseUrl      = TableAPIBaseURL + "/label"
	LabelEntryBaseUrl = TableAPIBaseURL + "/label_entry"

	// To get possible states for service catalog requested item.
	ChoiceBaseUrl = TableAPIBaseURL + "/sys_choice"

	InstanceURLTemplate = `{{.Deployment}}.service-now.com`

	// DictionaryBaseUrl is the sys_dictionary table: the data dictionary. Each row
	// describes a table (when its `element` is empty — the table-level collection
	// record) or a column. Its `audit` flag is the connector-readable signal for
	// whether field-change auditing is enabled (drives sys_audit), used by the
	// event-feed audit-config preflight.
	DictionaryBaseUrl = TableAPIBaseURL + "/sys_dictionary"

	// PropertyBaseUrl is the sys_properties table (system properties). The
	// event-feed revoke-detection preflight reads the glide.ui.audit_deleted_tables
	// row from it: the comma-separated list of tables whose hard deletes are
	// captured to sys_audit_delete (the grant-REVOKE source).
	PropertyBaseUrl = TableAPIBaseURL + "/sys_properties"

	// AuditDeletedTablesProperty is the sys_properties name whose value lists the
	// tables whose deletes are recorded in sys_audit_delete.
	AuditDeletedTablesProperty = "glide.ui.audit_deleted_tables"

	// Variable sets & variables (Table API).
	VariableSetM2MBaseUrl = TableAPIBaseURL + "/io_set_item"
	ItemOptionNewBaseUrl  = TableAPIBaseURL + "/item_option_new" // variables (questions)
	QuestionChoiceBaseUrl = TableAPIBaseURL + "/question_choice" // option lists
)

type ListResponse[T any] struct {
	Result []T `json:"result"`
}

type SingleResponse[T any] struct {
	Result T `json:"result"`
}

type IDResponse = SingleResponse[BaseResource]
type UserResponse = SingleResponse[User]
type UsersResponse = ListResponse[User]
type RolesResponse = ListResponse[Role]
type GroupsResponse = ListResponse[Group]
type GroupResponse = SingleResponse[Group]
type GroupMembersResponse = ListResponse[GroupMember]
type UserRolesResponse = SingleResponse[UserRoles]
type UserToRoleResponse ListResponse[UserToRole]
type GroupToRoleResponse ListResponse[GroupToRole]
type CatalogsResponse = ListResponse[Catalog]
type CategoriesResponse = ListResponse[Category]
type CatalogItemsResponse = ListResponse[CatalogItem]
type CatalogItemResponse = SingleResponse[CatalogItem]
type CatalogItemVariablesResponse = ListResponse[CatalogItemVariable]
type OrderCatalogItemResponse = SingleResponse[RequestInfo]
type RequestItemResponse = SingleResponse[RequestedItem]
type RequestItemsResponse = ListResponse[RequestedItem]
type ServiceCatalogRequestResponse = SingleResponse[ServiceCatalogRequest]
type LabelResponse = SingleResponse[Label]
type LabelsResponse = ListResponse[Label]
type LabelEntriesLabelNameResponse = ListResponse[LabelEntryName]
type RequestedItemStateResponse = ListResponse[RequestItemState]
type VariableSetM2MResponse = ListResponse[VariableSetM2M]
type VariableSetsResponse = ListResponse[VariableSet]
type ItemOptionNewResponse = ListResponse[ItemOptionNew]
type QuestionChoiceResponse = ListResponse[QuestionChoice]

type Client struct {
	httpClient          *http.Client
	auth                string
	deployment          string
	baseURL             string
	baseURLOverride     bool
	TicketSchemaFilters map[string]string
	AllowedDomains      []string
	CustomUserFields    []string
}

// Official documentation.
// https://developer.servicenow.com/dev.do#!/reference/api/rome/rest/c_TableAPI .
// https://www.servicenow.com/docs/bundle/yokohama-api-reference/page/integrate/inbound-rest/concept/c_TableAPI.html .
// https://developer.servicenow.com/dev.do#!/reference/api/yokohama/rest/c_TableAPI?navFilter=table .

func NewClient(
	httpClient *http.Client,
	auth string,
	deployment string,
	ticketSchemaFilters map[string]string,
	allowedDomains []string,
	customUserFields []string,
	baseURLOverride string,
) (*Client, error) {
	var baseURL string
	if baseURLOverride != "" {
		baseURL = baseURLOverride
	} else {
		var err error
		baseURL, err = GenerateURL(InstanceURLTemplate, map[string]string{"Deployment": deployment})
		if err != nil {
			return nil, err
		}
	}
	return &Client{
		httpClient:          httpClient,
		auth:                auth,
		deployment:          deployment,
		baseURL:             baseURL,
		baseURLOverride:     baseURLOverride != "",
		TicketSchemaFilters: ticketSchemaFilters,
		AllowedDomains:      allowedDomains,
		CustomUserFields:    customUserFields,
	}, nil
}

func (c *Client) GetBaseURL() string {
	return c.baseURL
}

// AuditDeletePayloadFields is the field set fetched from sys_audit_delete for
// the event feed. It adds `payload` (the full XML dump of the deleted row) on
// top of AuditDeleteFields so the feed can resolve a deleted join row back to
// its (principal, target) pair and emit a revoke event.
var AuditDeletePayloadFields = []string{"tablename", fieldDocumentKey, "sys_created_on", "payload"}

// GetDeletedSincePayload lists sys_audit_delete rows for one or more tables
// whose delete was logged at or after createdSince ("YYYY-MM-DD HH:MM:SS",
// UTC), INCLUDING the `payload` column and ordered by sys_created_on ascending
// so the event feed advances its cursor monotonically. An empty createdSince
// fetches from the beginning of the delete history. The returned next-page
// token is the Table API offset for the following page ("" when exhausted).
//
// This is the event-feed delete source (it mirrors GetAuditSince's ordering and
// GetDeletedSince's filtering, but additionally fetches the payload so deletes
// of join rows become resolvable revoke events). Errors are surfaced to the
// caller; the feed degrades gracefully and the periodic full sync remains the
// source of truth.
func (c *Client) GetDeletedSincePayload(ctx context.Context, tableNames []string, createdSince string, paginationVars PaginationVars) ([]AuditDeleteRecord, string, error) {
	var resp AuditDeleteResponse

	var query string
	switch {
	case len(tableNames) == 1:
		query = fmt.Sprintf("tablename=%s", tableNames[0])
	case len(tableNames) > 1:
		query = "tablenameIN" + strings.Join(tableNames, ",")
	}
	if createdSince != "" {
		clause := fmt.Sprintf("sys_created_on>=%s", createdSince)
		if query != "" {
			query += "^" + clause
		} else {
			query = clause
		}
	}
	// Order ascending so paging walks forward in time from the cursor.
	if query != "" {
		query += "^ORDERBYsys_created_on"
	} else {
		query = "ORDERBYsys_created_on"
	}

	reqOpts := []ReqOpt{
		WithQuery(query),
		WithFields(AuditDeletePayloadFields...),
	}
	reqOpts = append(reqOpts, paginationVarsToReqOptions(&paginationVars)...)

	nextPage, err := c.get(ctx, c.apiURL(AuditDeleteBaseUrl, c.deployment), &resp, reqOpts...)
	if err != nil {
		return nil, "", err
	}
	return resp.Result, nextPage, nil
}

// fieldSysID/fieldUser are ServiceNow columns requested in the sysparm_fields
// of the event-feed list calls.
const (
	fieldSysID       = "sys_id"
	fieldUser        = "user"
	fieldDocumentKey = "documentkey"
)

// AuditFields is the field set fetched from sys_audit for the event feed.
var AuditFields = []string{fieldSysID, "tablename", fieldDocumentKey, "fieldname", "oldvalue", "newvalue", "sys_created_on", fieldUser}

// GetAuditSince lists sys_audit rows for one or more tables whose change was
// logged at or after createdSince ("YYYY-MM-DD HH:MM:SS", UTC), ordered by
// sys_created_on ascending so the event feed advances its cursor monotonically.
// An empty createdSince fetches from the beginning of the audit history. The
// returned next-page token is the Table API offset for the following page (""
// when exhausted).
//
// Note: an error here (e.g. auditing disabled, or the caller lacks read access
// to sys_audit) is surfaced to the caller; the event feed degrades gracefully
// and the periodic full sync remains the source of truth.
func (c *Client) GetAuditSince(ctx context.Context, tableNames []string, createdSince string, paginationVars PaginationVars) ([]AuditRecord, string, error) {
	var resp AuditResponse

	var query string
	switch {
	case len(tableNames) == 1:
		query = fmt.Sprintf("tablename=%s", tableNames[0])
	case len(tableNames) > 1:
		query = "tablenameIN" + strings.Join(tableNames, ",")
	}
	if createdSince != "" {
		clause := fmt.Sprintf("sys_created_on>=%s", createdSince)
		if query != "" {
			query += "^" + clause
		} else {
			query = clause
		}
	}
	// Order ascending so paging walks forward in time from the cursor.
	if query != "" {
		query += "^ORDERBYsys_created_on"
	} else {
		query = "ORDERBYsys_created_on"
	}

	reqOpts := []ReqOpt{
		WithQuery(query),
		WithFields(AuditFields...),
	}
	reqOpts = append(reqOpts, paginationVarsToReqOptions(&paginationVars)...)

	nextPage, err := c.get(ctx, c.apiURL(AuditBaseUrl, c.deployment), &resp, reqOpts...)
	if err != nil {
		return nil, "", err
	}
	return resp.Result, nextPage, nil
}

// GetTableAuditFlags reports, for each of the given table names, whether
// field-change auditing is enabled on that table. It reads the table-level
// collection record from sys_dictionary (the row where `element` is empty) and
// returns a map tableName -> auditEnabled.
//
// The flag reflects sys_dictionary's `audit` column ("true"/"false"); a table
// absent from the result (no table-level record returned, or unreadable) is
// reported as false. This signals whether sys_audit field-change rows will be
// produced — it does NOT reflect delete-recovery (sys_audit_delete), which is a
// separate, independently-configured capability. Callers must treat this as
// advisory only.
func (c *Client) GetTableAuditFlags(ctx context.Context, tableNames []string) (map[string]bool, error) {
	out := make(map[string]bool, len(tableNames))
	if len(tableNames) == 0 {
		return out, nil
	}

	var resp DictionaryResponse
	// Only the table-level record carries the table's audit flag: name IN (...)
	// AND element is empty.
	query := "nameIN" + strings.Join(tableNames, ",") + "^elementISEMPTY"
	reqOpts := []ReqOpt{
		WithQuery(query),
		WithFields("name", "element", "audit"),
	}
	pv := PaginationVars{Limit: len(tableNames) + 1}
	reqOpts = append(reqOpts, paginationVarsToReqOptions(&pv)...)

	_, err := c.get(ctx, c.apiURL(DictionaryBaseUrl, c.deployment), &resp, reqOpts...)
	if err != nil {
		return nil, fmt.Errorf("baton-servicenow: failed to read sys_dictionary audit flags: %w", err)
	}

	for i := range resp.Result {
		r := &resp.Result[i]
		if r.Element != "" {
			continue
		}
		out[r.Name] = boolStr(r.Audit)
	}
	return out, nil
}

// GetAuditDeletedTables reads the glide.ui.audit_deleted_tables system property
// (sys_properties) and returns the list of table names whose hard deletes are
// captured to sys_audit_delete. That capture is the source of near-real-time
// grant-REVOKE events, so a join table's PRESENCE in this list means its revokes
// are reliably captured.
//
// Caveat (do NOT treat absence as definitive): some tables can have their
// deletes captured to sys_audit_delete by their own mechanism without appearing
// in this property. So presence => reliably captured; absence => not reliably
// captured via this property (but may still be captured by other means).
// Callers must treat this as advisory only.
//
// An empty / unset property yields an empty list (no error). Errors (e.g. lack
// of read access to sys_properties) are surfaced to the caller, which degrades
// gracefully.
func (c *Client) GetAuditDeletedTables(ctx context.Context) ([]string, error) {
	var resp PropertyResponse
	reqOpts := []ReqOpt{
		WithQuery("name=" + AuditDeletedTablesProperty),
		WithFields("name", "value"),
	}
	pv := PaginationVars{Limit: 1}
	reqOpts = append(reqOpts, paginationVarsToReqOptions(&pv)...)

	_, err := c.get(ctx, c.apiURL(PropertyBaseUrl, c.deployment), &resp, reqOpts...)
	if err != nil {
		return nil, fmt.Errorf("baton-servicenow: failed to read %s system property: %w", AuditDeletedTablesProperty, err)
	}

	var tables []string
	for i := range resp.Result {
		if resp.Result[i].Name != AuditDeletedTablesProperty {
			continue
		}
		for _, t := range strings.Split(resp.Result[i].Value, ",") {
			if t = strings.TrimSpace(t); t != "" {
				tables = append(tables, t)
			}
		}
	}
	return tables, nil
}

// CreatedSinceField is the ServiceNow column used as the event-feed cursor for
// immutable join/assignment rows (which have no meaningful sys_updated_on).
const CreatedSinceField = "sys_created_on"

// appendCreatedSince ANDs a "sys_created_on>=<ts>^ORDERBYsys_created_on" clause
// onto an existing query, ordering ascending so paging walks forward in time.
// An empty ts orders without a lower bound (full pull).
func appendCreatedSince(query string, ts string) string {
	if ts != "" {
		clause := fmt.Sprintf("%s>=%s", CreatedSinceField, ts)
		if query != "" {
			query += "^" + clause
		} else {
			query = clause
		}
	}
	if query != "" {
		return query + "^ORDERBY" + CreatedSinceField
	}
	return "ORDERBY" + CreatedSinceField
}

// GetGroupMembersCreatedSince lists sys_user_grmember rows created at or after
// createdSince, ordered ascending. Each row's user/group are the principal and
// target of a group-membership grant.
func (c *Client) GetGroupMembersCreatedSince(ctx context.Context, createdSince string, paginationVars PaginationVars) ([]GroupMember, string, error) {
	var resp GroupMembersResponse
	reqOpts := []ReqOpt{
		WithQuery(appendCreatedSince("", createdSince)),
		WithFields(fieldSysID, fieldUser, "group", "sys_created_on"),
	}
	reqOpts = append(reqOpts, paginationVarsToReqOptions(&paginationVars)...)
	next, err := c.get(ctx, c.apiURL(GroupMembersBaseUrl, c.deployment), &resp, reqOpts...)
	if err != nil {
		return nil, "", err
	}
	return resp.Result, next, nil
}

// GetUserRolesCreatedSince lists sys_user_has_role rows created at or after
// createdSince, ordered ascending (user/role = principal/target of a grant).
func (c *Client) GetUserRolesCreatedSince(ctx context.Context, createdSince string, paginationVars PaginationVars) ([]UserToRole, string, error) {
	var resp ListResponse[UserToRole]
	reqOpts := []ReqOpt{
		WithQuery(appendCreatedSince("", createdSince)),
		WithFields(fieldSysID, fieldUser, "role", "inherited", "sys_created_on"),
	}
	reqOpts = append(reqOpts, paginationVarsToReqOptions(&paginationVars)...)
	next, err := c.get(ctx, c.apiURL(UserRolesBaseUrl, c.deployment), &resp, reqOpts...)
	if err != nil {
		return nil, "", err
	}
	return resp.Result, next, nil
}

// GetGroupRolesCreatedSince lists sys_group_has_role rows created at or after
// createdSince, ordered ascending (group/role = principal/target of a grant).
func (c *Client) GetGroupRolesCreatedSince(ctx context.Context, createdSince string, paginationVars PaginationVars) ([]GroupToRole, string, error) {
	var resp ListResponse[GroupToRole]
	reqOpts := []ReqOpt{
		WithQuery(appendCreatedSince("", createdSince)),
		WithFields(fieldSysID, "group", "role", "inherits", "sys_created_on"),
	}
	reqOpts = append(reqOpts, paginationVarsToReqOptions(&paginationVars)...)
	next, err := c.get(ctx, c.apiURL(GroupRolesBaseUrl, c.deployment), &resp, reqOpts...)
	if err != nil {
		return nil, "", err
	}
	return resp.Result, next, nil
}

// apiURL builds an API URL from a constant pattern like UsersBaseUrl.
// When a base URL override is set, it replaces the default
// https://DEPLOYMENT.service-now.com/api prefix with the override.
func (c *Client) apiURL(pattern string, args ...any) string {
	expanded := fmt.Sprintf(pattern, args...)
	if c.baseURLOverride {
		defaultBase := fmt.Sprintf("https://%s.service-now.com/api", c.deployment)
		return strings.Replace(expanded, defaultBase, c.baseURL, 1)
	}
	return expanded
}

// Table `sys_user` (Users).
func (c *Client) GetUsers(ctx context.Context, paginationVars PaginationVars) ([]User, string, error) {
	return c.GetUsersUpdatedSince(ctx, paginationVars, "")
}

// GetUsersUpdatedSince lists users optionally restricted to those whose
// sys_updated_on is at or after updatedSince. When updatedSince is empty it
// behaves identically to GetUsers (full pull). The event feed's account-change
// phase passes the cursor watermark to surface created/modified/disabled accounts.
func (c *Client) GetUsersUpdatedSince(ctx context.Context, paginationVars PaginationVars, updatedSince string) ([]User, string, error) {
	var usersResponse UsersResponse

	filters := prepareUserFilters(c.AllowedDomains, c.CustomUserFields)
	filters.Query = appendUpdatedSince(filters.Query, updatedSince)
	reqOpts := filterToReqOptions(filters)
	reqOpts = append(reqOpts, paginationVarsToReqOptions(&paginationVars)...)

	nextPage, err := c.get(
		ctx,
		c.apiURL(UsersBaseUrl, c.deployment),
		&usersResponse,
		reqOpts...,
	)

	if err != nil {
		return nil, "", err
	}

	return usersResponse.Result, nextPage, nil
}

func (c *Client) GetUser(ctx context.Context, userId string) (*User, error) {
	var userResponse UserResponse

	_, err := c.get(
		ctx,
		c.apiURL(UserBaseUrl, c.deployment, userId),
		&userResponse,
		WithFields(UserFields...),
	)

	if err != nil {
		return nil, err
	}

	return &userResponse.Result, nil
}

// Table `sys_user_group` (Groups).
func (c *Client) GetGroups(ctx context.Context, paginationVars PaginationVars, groupIDs []string) ([]Group, string, error) {
	return c.GetGroupsUpdatedSince(ctx, paginationVars, groupIDs, "")
}

// GetGroupsUpdatedSince lists groups optionally restricted to those updated at
// or after updatedSince. Empty updatedSince == full pull.
func (c *Client) GetGroupsUpdatedSince(ctx context.Context, paginationVars PaginationVars, groupIDs []string, updatedSince string) ([]Group, string, error) {
	var groupsResponse GroupsResponse

	filters := prepareGroupFilters(groupIDs)
	filters.Query = appendUpdatedSince(filters.Query, updatedSince)
	reqOpts := filterToReqOptions(filters)
	reqOpts = append(reqOpts, paginationVarsToReqOptions(&paginationVars)...)
	nextPageToken, err := c.get(
		ctx,
		c.apiURL(GroupsBaseUrl, c.deployment),
		&groupsResponse,
		reqOpts...,
	)

	if err != nil {
		return nil, "", err
	}

	return groupsResponse.Result, nextPageToken, nil
}

func (c *Client) GetGroup(ctx context.Context, groupId string) (*Group, error) {
	var groupResponse GroupResponse

	_, err := c.get(
		ctx,
		c.apiURL(GroupBaseUrl, c.deployment, groupId),
		&groupResponse,
		WithFields(GroupFields...),
	)

	if err != nil {
		return nil, err
	}

	return &groupResponse.Result, nil
}

// Table `sys_user_grmember` (Group Members).
func (c *Client) GetUserToGroup(ctx context.Context, userId string, groupId string, paginationVars PaginationVars) ([]GroupMember, string, error) {
	return c.GetUserToGroupUpdatedSince(ctx, userId, groupId, paginationVars, "")
}

// GetUserToGroupUpdatedSince lists group-membership rows optionally restricted
// to those updated at or after updatedSince. Empty updatedSince == full pull.
func (c *Client) GetUserToGroupUpdatedSince(ctx context.Context, userId string, groupId string, paginationVars PaginationVars, updatedSince string) ([]GroupMember, string, error) {
	var groupMembersResponse GroupMembersResponse

	filters := prepareUserToGroupFilter(userId, groupId)
	filters.Query = appendUpdatedSince(filters.Query, updatedSince)
	reqOpts := filterToReqOptions(filters)
	reqOpts = append(reqOpts, paginationVarsToReqOptions(&paginationVars)...)

	nextPageToken, err := c.get(
		ctx,
		c.apiURL(GroupMembersBaseUrl, c.deployment),
		&groupMembersResponse,
		reqOpts...,
	)

	if err != nil {
		return nil, "", err
	}

	return groupMembersResponse.Result, nextPageToken, nil
}

func (c *Client) AddUserToGroup(ctx context.Context, record GroupMemberPayload) error {
	return c.post(
		ctx,
		c.apiURL(GroupMembersBaseUrl, c.deployment),
		nil,
		&record,
		WithIncludeResponseBody(),
	)
}

func (c *Client) RemoveUserFromGroup(ctx context.Context, id string) error {
	return c.delete(
		ctx,
		c.apiURL(GroupMemberDetailBaseUrl, c.deployment, id),
		nil,
	)
}

// Table `sys_user_role` (Roles).
func (c *Client) GetRoles(ctx context.Context, paginationVars PaginationVars) ([]Role, string, error) {
	return c.GetRolesUpdatedSince(ctx, paginationVars, "")
}

// GetRolesUpdatedSince lists roles optionally restricted to those updated at or
// after updatedSince. Empty updatedSince == full pull.
func (c *Client) GetRolesUpdatedSince(ctx context.Context, paginationVars PaginationVars, updatedSince string) ([]Role, string, error) {
	var rolesResponse RolesResponse

	paginationVars.Limit++
	filters := prepareRoleFilters()
	filters.Query = appendUpdatedSince(filters.Query, updatedSince)
	reqOpts := filterToReqOptions(filters)
	reqOpts = append(reqOpts, paginationVarsToReqOptions(&paginationVars)...)

	nextPageToken, err := c.get(
		ctx,
		c.apiURL(RolesBaseUrl, c.deployment),
		&rolesResponse,
		reqOpts...,
	)

	if err != nil {
		return nil, "", err
	}

	return rolesResponse.Result, nextPageToken, nil
}

// Table `sys_user_has_role` (User to Role).
func (c *Client) GetUserToRole(ctx context.Context, userId string, roleId string, paginationVars PaginationVars) ([]UserToRole, string, error) {
	return c.GetUserToRoleUpdatedSince(ctx, userId, roleId, paginationVars, "")
}

// GetUserToRoleUpdatedSince lists user-role rows optionally restricted to those
// updated at or after updatedSince. Empty updatedSince == full pull.
func (c *Client) GetUserToRoleUpdatedSince(ctx context.Context, userId string, roleId string, paginationVars PaginationVars, updatedSince string) ([]UserToRole, string, error) {
	var userToRoleResponse UserToRoleResponse

	filters := prepareUserToRoleFilter(userId, roleId)
	filters.Query = appendUpdatedSince(filters.Query, updatedSince)
	reqOpts := filterToReqOptions(filters)
	reqOpts = append(reqOpts, paginationVarsToReqOptions(&paginationVars)...)

	nextPageToken, err := c.get(
		ctx,
		c.apiURL(UserRolesBaseUrl, c.deployment),
		&userToRoleResponse,
		reqOpts...,
	)

	if err != nil {
		return nil, "", err
	}

	return userToRoleResponse.Result, nextPageToken, nil
}

func (c *Client) GrantRoleToUser(ctx context.Context, record UserToRolePayload) error {
	return c.post(
		ctx,
		c.apiURL(UserRolesBaseUrl, c.deployment),
		nil,
		&record,
		WithIncludeResponseBody(),
	)
}

func (c *Client) RevokeRoleFromUser(ctx context.Context, id string) error {
	return c.delete(
		ctx,
		c.apiURL(UserRoleDetailBaseUrl, c.deployment, id),
		nil,
	)
}

// Table `sys_group_has_role` (Group to Role).
func (c *Client) GetGroupToRole(ctx context.Context, groupId string, roleId string, paginationVars PaginationVars) ([]GroupToRole, string, error) {
	return c.GetGroupToRoleUpdatedSince(ctx, groupId, roleId, paginationVars, "")
}

// GetGroupToRoleUpdatedSince lists group-role rows optionally restricted to
// those updated at or after updatedSince. Empty updatedSince == full pull.
func (c *Client) GetGroupToRoleUpdatedSince(ctx context.Context, groupId string, roleId string, paginationVars PaginationVars, updatedSince string) ([]GroupToRole, string, error) {
	var groupToRoleResponse GroupToRoleResponse

	filters := prepareGroupToRoleFilter(groupId, roleId)
	filters.Query = appendUpdatedSince(filters.Query, updatedSince)
	reqOpts := filterToReqOptions(filters)
	reqOpts = append(reqOpts, paginationVarsToReqOptions(&paginationVars)...)
	nextPageToken, err := c.get(
		ctx,
		c.apiURL(GroupRolesBaseUrl, c.deployment),
		&groupToRoleResponse,
		reqOpts...,
	)

	if err != nil {
		return nil, "", err
	}

	return groupToRoleResponse.Result, nextPageToken, nil
}

func (c *Client) GrantRoleToGroup(ctx context.Context, record GroupToRolePayload) error {
	return c.post(
		ctx,
		c.apiURL(GroupRolesBaseUrl, c.deployment),
		nil,
		&record,
		WithIncludeResponseBody(),
	)
}

func (c *Client) RevokeRoleFromGroup(ctx context.Context, id string) error {
	return c.delete(
		ctx,
		c.apiURL(GroupRoleDetailBaseUrl, c.deployment, id),
		nil,
	)
}

func (c *Client) get(ctx context.Context, urlAddress string, resourceResponse interface{}, reqOptions ...ReqOpt) (string, error) {
	return c.doRequestWithRetry(
		ctx,
		urlAddress,
		http.MethodGet,
		nil,
		&resourceResponse,
		reqOptions...,
	)
}

func (c *Client) post(
	ctx context.Context,
	urlAddress string,
	resourceResponse interface{},
	data interface{},
	requestOptions ...ReqOpt,
) error {
	_, err := c.doRequestWithRetry(
		ctx,
		urlAddress,
		http.MethodPost,
		data,
		&resourceResponse,
		requestOptions...,
	)

	return err
}

func (c *Client) patch(
	ctx context.Context,
	urlAddress string,
	resourceResponse interface{},
	data interface{},
	requestOptions ...ReqOpt,
) error {
	_, err := c.doRequestWithRetry(
		ctx,
		urlAddress,
		http.MethodPatch,
		data,
		&resourceResponse,
		requestOptions...,
	)

	return err
}

func (c *Client) delete(
	ctx context.Context,
	urlAddress string,
	resourceResponse interface{},
	reqOptions ...ReqOpt,
) error {
	_, err := c.doRequestWithRetry(
		ctx,
		urlAddress,
		http.MethodDelete,
		nil,
		&resourceResponse,
		reqOptions...,
	)

	return err
}

func handleStatusCode(statusCode int) codes.Code {
	switch statusCode {
	case http.StatusRequestTimeout:
		return codes.DeadlineExceeded
	case http.StatusTooManyRequests, http.StatusBadGateway, http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return codes.Unavailable
	case http.StatusNotFound:
		return codes.NotFound
	case http.StatusUnauthorized:
		return codes.Unauthenticated
	case http.StatusForbidden:
		return codes.PermissionDenied
	case http.StatusConflict:
		return codes.AlreadyExists
	case http.StatusNotImplemented:
		return codes.Unimplemented
	}

	if statusCode >= 500 && statusCode <= 599 {
		return codes.Unavailable
	}

	if statusCode < 200 || statusCode >= 300 {
		return codes.Unknown
	}

	return codes.OK
}

func (c *Client) doRequest(ctx context.Context, urlAddress string, method string, data any, resourceResponse any, reqOptions ...ReqOpt) (string, error) {
	var body io.Reader

	if data != nil {
		jsonBody, err := json.Marshal(data)
		if err != nil {
			return "", err
		}

		body = bytes.NewBuffer(jsonBody)
	}

	req, err := http.NewRequestWithContext(ctx, method, urlAddress, body)
	if err != nil {
		return "", err
	}

	// Set default value
	WithQueryParam("sysparm_exclude_reference_link", "true")(req)

	req.Header.Set("Authorization", c.auth)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if method == http.MethodPost || method == http.MethodPatch {
		req.Header.Set("X-no-response-body", "true")
	}

	for _, o := range reqOptions {
		o(req)
	}

	req.URL.RawQuery = req.URL.Query().Encode()

	rawResponse, err := c.httpClient.Do(req)
	if rawResponse != nil && rawResponse.Body != nil {
		defer rawResponse.Body.Close()
	}
	if err != nil {
		return "", err
	}

	if rawResponse.StatusCode < 0 || rawResponse.StatusCode > math.MaxUint32 {
		return "", errors.New("status code is out of range for uint32")
	}

	if rawResponse.StatusCode >= 300 {
		respBody, _ := io.ReadAll(rawResponse.Body)
		return "", status.Errorf(handleStatusCode(rawResponse.StatusCode), "baton-servicenow: request failed with status %d: %s", rawResponse.StatusCode, string(respBody))
	}

	if method != http.MethodDelete {
		if err := json.NewDecoder(rawResponse.Body).Decode(&resourceResponse); err != nil {
			return "", err
		}
	}

	totalCountHeader := rawResponse.Header.Get("X-Total-Count")
	totalCount, err := ConvertPageToken(totalCountHeader)
	if err != nil {
		return "", err
	}

	var pageToken string
	pagingLinks := linkheader.Parse(rawResponse.Header.Get("Link"))
	for _, link := range pagingLinks {
		if link.Rel == "next" {
			nextPageUrl, err := url.Parse(link.URL)
			if err != nil {
				return "", err
			}
			offset := nextPageUrl.Query().Get("sysparm_offset")
			token, err := ConvertPageToken(offset)
			if err != nil {
				return "", err
			}
			if token < totalCount {
				pageToken = offset
			}
			break
		}
	}

	return pageToken, nil
}

const (
	maxAuthRetries    = 3
	authRetryBaseWait = time.Second
)

// doRequestWithRetry wraps doRequest with a small retry loop for transient 401 responses.
// On each 401 it logs the ServiceNow error body and waits an increasing delay before retrying.
func (c *Client) doRequestWithRetry(ctx context.Context, urlAddress string, method string, data any, resourceResponse any, reqOptions ...ReqOpt) (string, error) {
	l := ctxzap.Extract(ctx)

	var lastErr error
	for attempt := 1; attempt <= maxAuthRetries+1; attempt++ {
		if attempt > 1 {
			delay := time.Duration(attempt-1) * authRetryBaseWait
			l.Debug("baton-servicenow: retrying request after 401",
				zap.String("url", urlAddress),
				zap.String("method", method),
				zap.Int("attempt", attempt),
				zap.Int("max_attempts", maxAuthRetries+1),
				zap.Duration("delay", delay),
			)
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}

		pageToken, err := c.doRequest(ctx, urlAddress, method, data, resourceResponse, reqOptions...)
		if err == nil {
			if attempt > 1 {
				l.Debug("baton-servicenow: request succeeded after retry",
					zap.String("url", urlAddress),
					zap.String("method", method),
					zap.Int("attempt", attempt),
				)
			}
			return pageToken, nil
		}

		if status.Code(err) != codes.Unauthenticated {
			return "", err
		}

		l.Debug("baton-servicenow: received 401 unauthorized",
			zap.String("url", urlAddress),
			zap.String("method", method),
			zap.Int("attempt", attempt),
			zap.Int("max_attempts", maxAuthRetries+1),
			zap.Error(err),
		)
		lastErr = err
	}

	l.Debug("baton-servicenow: request failed after all retry attempts",
		zap.String("url", urlAddress),
		zap.String("method", method),
		zap.Int("max_attempts", maxAuthRetries+1),
		zap.Error(lastErr),
	)
	return "", lastErr
}

func (c *Client) CreateUserAccount(ctx context.Context, user any) (*User, error) {
	var response UserResponse

	err := c.post(
		ctx,
		c.apiURL(UsersBaseUrl, c.deployment),
		&response,
		user,
		WithIncludeResponseBody(),
	)

	if err != nil {
		return nil, fmt.Errorf("failed to create user in ServiceNow: %w", err)
	}

	return &response.Result, nil
}

func (c *Client) UpdateUserActiveStatus(ctx context.Context, userId string, active bool) (*User, error) {
	payload := map[string]bool{
		"active": active,
	}

	var response UserResponse
	err := c.patch(
		ctx,
		c.apiURL(UserBaseUrl, c.deployment, userId),
		&response,
		payload,
		WithIncludeResponseBody(),
	)

	if err != nil {
		return nil, fmt.Errorf("failed to update user active status in ServiceNow: %w", err)
	}

	return &response.Result, nil
}

// Includes variables that come from variable sets (Table API -> item_option_new) and choices for those set variables.
func (c *Client) GetCatalogItemVariablesPlusSets(ctx context.Context, itemSysID string) ([]CatalogItemVariable, error) {
	itemVars, err := c.GetCatalogItemVariables(ctx, itemSysID)
	if err != nil {
		return nil, fmt.Errorf("failed to get item variables: %w", err)
	}

	// Find attached variable sets
	links, _, err := c.GetVariableSetLinksForItem(ctx, itemSysID, PaginationVars{Limit: 200})
	if err != nil {
		return nil, fmt.Errorf("failed to get variable set links: %w", err)
	}
	if len(links) == 0 {
		return itemVars, nil // nothing to add
	}

	setIDs := make([]string, 0, len(links))
	for _, l := range links {
		setIDs = append(setIDs, l.VariableSet)
	}

	// Fetch variables that belong to those sets
	setVars, _, err := c.GetVariablesBySetIDs(ctx, setIDs, PaginationVars{Limit: 500})
	if err != nil {
		return nil, fmt.Errorf("failde to get variables by set ids: %w", err)
	}

	// Fetch choices for set variables (so selects have options)
	varIDs := make([]string, 0, len(setVars))
	for _, v := range setVars {
		varIDs = append(varIDs, v.SysID)
	}
	choices, _, err := c.GetChoicesForVariables(ctx, varIDs, PaginationVars{Limit: 1000})
	if err != nil {
		return nil, fmt.Errorf("failed to get choices for set variables: %w", err)
	}
	choicesByQ := make(map[string][]QuestionChoice, len(varIDs))
	for _, ch := range choices {
		choicesByQ[ch.Question] = append(choicesByQ[ch.Question], ch)
	}

	// Map set variables to CatalogItemVariable
	cvSet := make([]CatalogItemVariable, 0, len(setVars))
	for _, v := range setVars {
		cvSet = append(cvSet, MapItemOptionNewToCatalogItemVariable(v, choicesByQ[v.SysID]))
	}

	// Merge (prefer direct items on ID collisions)
	out := make([]CatalogItemVariable, 0, len(itemVars)+len(cvSet))
	seen := make(map[string]struct{}, len(itemVars))
	for _, v := range itemVars {
		out = append(out, v)
		seen[v.ID] = struct{}{}
	}
	for _, v := range cvSet {
		if _, dup := seen[v.ID]; !dup {
			out = append(out, v)
		}
	}

	return out, nil
}

func (c *Client) GetVariableSetLinksForItem(ctx context.Context, itemSysID string, pg PaginationVars) ([]VariableSetM2M, string, error) {
	var resp VariableSetM2MResponse
	req := []ReqOpt{
		WithQueryParam("sysparm_query", fmt.Sprintf("sc_cat_item=%s", itemSysID)),
		WithQueryParam("sysparm_fields", "sys_id,variable_set"),
		WithQueryParam("sysparm_exclude_reference_link", "true"),
	}
	req = append(req, paginationVarsToReqOptions(&pg)...)

	next, err := c.get(ctx, c.apiURL(VariableSetM2MBaseUrl, c.deployment), &resp, req...)
	if err != nil {
		return nil, "", err
	}
	return resp.Result, next, nil
}

func (c *Client) GetVariablesBySetIDs(ctx context.Context, setIDs []string, pg PaginationVars) ([]ItemOptionNew, string, error) {
	if len(setIDs) == 0 {
		return nil, "", nil
	}
	var resp ItemOptionNewResponse
	req := []ReqOpt{
		WithQueryParam("sysparm_query", "variable_setIN"+strings.Join(setIDs, ",")),
		WithQueryParam("sysparm_fields", "sys_id,name,question_text,type,mandatory,default_value,reference,attributes,active,cat_item,variable_set"),
		WithQueryParam("sysparm_exclude_reference_link", "true"),
	}
	req = append(req, paginationVarsToReqOptions(&pg)...)

	next, err := c.get(ctx, c.apiURL(ItemOptionNewBaseUrl, c.deployment), &resp, req...)
	if err != nil {
		return nil, "", err
	}
	return resp.Result, next, nil
}

func (c *Client) GetChoicesForVariables(ctx context.Context, varIDs []string, pg PaginationVars) ([]QuestionChoice, string, error) {
	if len(varIDs) == 0 {
		return nil, "", nil
	}
	var resp QuestionChoiceResponse
	req := []ReqOpt{
		WithQueryParam("sysparm_query", "questionIN"+strings.Join(varIDs, ",")),
		WithQueryParam("sysparm_fields", "sys_id,label,value,question"),
		WithQueryParam("sysparm_exclude_reference_link", "true"),
	}
	req = append(req, paginationVarsToReqOptions(&pg)...)

	next, err := c.get(ctx, c.apiURL(QuestionChoiceBaseUrl, c.deployment), &resp, req...)
	if err != nil {
		return nil, "", err
	}
	return resp.Result, next, nil
}

// Unused but consider switching to this to get both direct catalog item variables and variables from variable sets.
func (c *Client) GetVariablesForItem(ctx context.Context, itemSysID string, pg PaginationVars) ([]ItemOptionNew, string, error) {
	var resp ItemOptionNewResponse
	req := []ReqOpt{
		WithQueryParam("sysparm_query", fmt.Sprintf("cat_item=%s", itemSysID)),
		WithQueryParam("sysparm_fields", "sys_id,name,question_text,type,mandatory,default_value,reference,attributes,active,cat_item,variable_set"),
		WithQueryParam("sysparm_exclude_reference_link", "true"),
	}
	req = append(req, paginationVarsToReqOptions(&pg)...)

	next, err := c.get(ctx, c.apiURL(ItemOptionNewBaseUrl, c.deployment), &resp, req...)
	if err != nil {
		return nil, "", err
	}
	return resp.Result, next, nil
}
