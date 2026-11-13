package config

import (
	"github.com/conductorone/baton-sdk/pkg/field"
)

var (
	usernameField = field.StringField("username",
		field.WithRequired(true),
		field.WithDisplayName("Username"),
		field.WithDescription("Username of administrator used to connect to the ServiceNow API."))
	passwordField = field.StringField("password",
		field.WithRequired(true),
		field.WithIsSecret(true),
		field.WithDisplayName("Password"),
		field.WithDescription("Application password used to connect to the ServiceNow API."))
	deploymentField = field.StringField("deployment",
		field.WithRequired(true),
		field.WithDisplayName("Deployment"),
		field.WithDescription("ServiceNow deployment to connect to."))
	catalogField = field.StringField("catalog-id",
		field.WithDisplayName("Catalog ID"),
		field.WithDescription("ServiceNow catalog id to filter catalog items to"))
	categoryField = field.StringField("category-id",
		field.WithDisplayName("Category ID"),
		field.WithDescription("ServiceNow category id to filter catalog items to"))
	ticketSchemaIDsField = field.StringSliceField("ticket-schema-ids",
		field.WithDisplayName("Ticket schema IDs"),
		field.WithDescription("Optional ServiceNow catalog item (ticket schema) sys_IDs to offer as ticket schemas, in the order given; at most 100. "+
			"Cannot be combined with Catalog ID or Category ID."),
		field.WithPlaceholder("9de5069c5afe602b2ea0a04b66beb2c0"),
	)
	allowedDomainsField = field.StringSliceField("allowed-domains",
		field.WithDisplayName("Allowed email domains"),
		field.WithDescription("Limit syncing to users whose email ends with one of the specified domains"),
		field.WithDefaultValue([]string{}),
	)
	customUserFieldsField = field.StringSliceField("custom-user-fields",
		field.WithDisplayName("Custom user fields"),
		field.WithDescription("Additional custom user fields to sync (must start with u_ prefix, e.g., u_type, u_department)"),
		field.WithDefaultValue([]string{}),
	)
	externalTicketField  = field.TicketingField.ExportAs(field.ExportTargetGUI)
	twoStepCheckoutField = field.BoolField("two-step-checkout",
		field.WithDisplayName("Two-Step Checkout"),
		field.WithDescription("Enable ServiceNow Two-Step Checkout for external ticket requests. Requires access to the Service Catalog cart endpoints."),
		field.WithDefaultValue(false),
	)
	baseURLField = field.StringField("base-url",
		field.WithDescription("Override the ServiceNow API URL (for testing)"),
		field.WithHidden(true),
		field.WithExportTarget(field.ExportTargetCLIOnly),
	)
	insecureField = field.BoolField("insecure",
		field.WithDescription("Allow insecure TLS connections (for testing with self-signed certificates)"),
		field.WithDefaultValue(false),
		field.WithHidden(true),
		field.WithExportTarget(field.ExportTargetCLIOnly),
	)
)

// configurationFields defines the external configuration required for the connector to run.
var configurationFields = []field.SchemaField{
	usernameField,
	passwordField,
	deploymentField,
	catalogField,
	categoryField,
	ticketSchemaIDsField,
	allowedDomainsField,
	customUserFieldsField,
	externalTicketField,
	twoStepCheckoutField,
	baseURLField,
	insecureField,
}

var configRelations = []field.SchemaFieldRelationship{
	field.FieldsDependentOn(
		[]field.SchemaField{catalogField, categoryField, ticketSchemaIDsField, twoStepCheckoutField},
		[]field.SchemaField{externalTicketField},
	),
	// Pairwise rather than one three-field group: Catalog ID and Category ID may still be combined with each other.
	field.FieldsMutuallyExclusive(ticketSchemaIDsField, catalogField),
	field.FieldsMutuallyExclusive(ticketSchemaIDsField, categoryField),
}

//go:generate go run ./gen
var Config = field.NewConfiguration(
	configurationFields,
	field.WithConstraints(configRelations...),
	field.WithConnectorDisplayName("ServiceNow"),
	field.WithHelpUrl("/docs/baton/servicenow"),
	field.WithIconUrl("/static/app-icons/servicenow.svg"),
)
