package main

import (
	"context"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/spf13/viper"
)

var (
	usernameField   = field.StringField("username", field.WithRequired(true), field.WithDescription("Username of administrator used to connect to the ServiceNow API."))
	passwordField   = field.StringField("password", field.WithRequired(true), field.WithDescription("Application password used to connect to the ServiceNow API."))
	deploymentField = field.StringField("deployment", field.WithRequired(true), field.WithDescription("ServiceNow deployment to connect to."))
	catalogField    = field.StringField("catalog-id", field.WithDescription("ServiceNow catalog id to fetch catalog items"))
	categoryField   = field.StringField("category-id", field.WithDescription("ServiceNow category id catalog items"))
)

// configurationFields defines the external configuration required for the connector to run.
var configurationFields = []field.SchemaField{
	usernameField,
	passwordField,
	deploymentField,
	catalogField,
	categoryField,
}

var configRelations = []field.SchemaFieldRelationshipI{
	field.FieldsDependentOn([]field.SchemaField{field.ListTicketSchemasField}, []field.SchemaField{catalogField, categoryField}),
}

// validateConfig is run after the configuration is loaded, and should return an error if it isn't valid.
func validateConfig(ctx context.Context, v *viper.Viper) error {
	if v.GetString(usernameField.FieldName) == "" || v.GetString(passwordField.FieldName) == "" {
		return fmt.Errorf("username and password must be provided")
	}

	if v.GetString(deploymentField.FieldName) == "" {
		return fmt.Errorf("deployment must be provided")
	}

	return nil
}
