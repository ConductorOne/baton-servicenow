package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"

	configschema "github.com/conductorone/baton-sdk/pkg/config"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/conductorone/baton-servicenow/pkg/config"
	"github.com/conductorone/baton-servicenow/pkg/connector"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

var version = "dev"

func main() {
	ctx := context.Background()

	_, cmd, err := configschema.DefineConfiguration(ctx, "baton-servicenow", getConnector, config.Config)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	cmd.Version = version

	err = cmd.Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func constructAuth(snc *config.ServiceNow) (string, error) {
	credentials := fmt.Sprintf("%s:%s", snc.Username, snc.Password)
	encodedCredentials := base64.StdEncoding.EncodeToString([]byte(credentials))
	return fmt.Sprintf("Basic %s", encodedCredentials), nil
}

func getConnector(ctx context.Context, snc *config.ServiceNow) (types.ConnectorServer, error) {
	l := ctxzap.Extract(ctx)

	// compose the auth options
	auth, err := constructAuth(snc)
	if err != nil {
		return nil, err
	}

	ticketSchemaFilters := make(map[string]string)

	catalogId := snc.CatalogId
	if catalogId != "" {
		ticketSchemaFilters["sysparm_catalog"] = catalogId
	}

	categoryId := snc.CategoryId
	if categoryId != "" {
		ticketSchemaFilters["sysparm_category"] = categoryId
	}

	servicenowConnector, err := connector.New(ctx, auth, snc.Deployment, ticketSchemaFilters, snc.AllowedDomains, snc.BaseUrl)
	if err != nil {
		l.Error("error creating connector", zap.Error(err))
		return nil, err
	}

	opts := make([]connectorbuilder.Opt, 0)
	if snc.Ticketing {
		opts = append(opts, connectorbuilder.WithTicketingEnabled())
	}

	c, err := connectorbuilder.NewConnector(ctx, servicenowConnector, opts...)
	if err != nil {
		l.Error("error creating connector", zap.Error(err))
		return nil, err
	}

	return c, nil
}
