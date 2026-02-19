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
	allowedDomainsField = field.StringSliceField("allowed-domains",
		field.WithDisplayName("Allowed email domains"),
		field.WithDescription("Limit syncing to users whose email ends with one of the specified domains"),
		field.WithDefaultValue([]string{}),
	)
	baseURLField = field.StringField("base-url",
		field.WithDisplayName("Base URL"),
		field.WithDescription("Override the ServiceNow API URL (for testing)"),
		field.WithHidden(true),
		field.WithExportTarget(field.ExportTargetCLIOnly),
	)
	externalTicketField = field.TicketingField.ExportAs(field.ExportTargetGUI)
)

// configurationFields defines the external configuration required for the connector to run.
var configurationFields = []field.SchemaField{
	usernameField,
	passwordField,
	deploymentField,
	catalogField,
	categoryField,
	allowedDomainsField,
	baseURLField,
	externalTicketField,
}

var configRelations = []field.SchemaFieldRelationship{
	field.FieldsDependentOn([]field.SchemaField{catalogField, categoryField}, []field.SchemaField{externalTicketField}),
}

//go:generate go run ./gen
var Config = field.NewConfiguration(
	configurationFields,
	field.WithConstraints(configRelations...),
	field.WithConnectorDisplayName("ServiceNow"),
	field.WithHelpUrl("/docs/baton/servicenow"),
	field.WithIconUrl("/static/app-icons/servicenow.svg"),
)
