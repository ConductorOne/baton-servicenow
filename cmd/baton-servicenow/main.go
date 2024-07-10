package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"

	configschema "github.com/conductorone/baton-sdk/pkg/config"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/conductorone/baton-servicenow/pkg/connector"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/spf13/viper"
	"go.uber.org/zap"
)

var version = "dev"

func main() {
	ctx := context.Background()

	_, cmd, err := configschema.DefineConfiguration(ctx, "baton-servicenow", getConnector, configurationFields, nil)
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

func constructAuth(v *viper.Viper) (string, error) {
	if v.GetString(usernameField.FieldName) != "" {
		credentials := fmt.Sprintf("%s:%s", v.GetString(usernameField.FieldName), v.GetString(passwordField.FieldName))
		encodedCredentials := base64.StdEncoding.EncodeToString([]byte(credentials))

		return fmt.Sprintf("Basic %s", encodedCredentials), nil
	}

	return "", fmt.Errorf("invalid config")
}

func getConnector(ctx context.Context, v *viper.Viper) (types.ConnectorServer, error) {
	l := ctxzap.Extract(ctx)

	if err := validateConfig(ctx, v); err != nil {
		return nil, err
	}

	// compose the auth options
	auth, err := constructAuth(v)
	if err != nil {
		return nil, err
	}

	servicenowConnector, err := connector.New(ctx, auth, v.GetString(deploymentField.FieldName))
	if err != nil {
		l.Error("error creating connector", zap.Error(err))
		return nil, err
	}

	c, err := connectorbuilder.NewConnector(ctx, servicenowConnector)
	if err != nil {
		l.Error("error creating connector", zap.Error(err))
		return nil, err
	}

	return c, nil
}
