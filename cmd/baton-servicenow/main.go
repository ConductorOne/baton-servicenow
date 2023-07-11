package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"

	"github.com/ConductorOne/baton-servicenow/pkg/connector"
	"github.com/conductorone/baton-sdk/pkg/cli"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"github.com/conductorone/baton-sdk/pkg/types"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

var version = "dev"

func main() {
	ctx := context.Background()

	cfg := &config{}
	cmd, err := cli.NewCmd(ctx, "baton-servicenow", cfg, validateConfig, getConnector)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	cmd.Version = version
	cmdFlags(cmd)

	err = cmd.Execute()
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func constructAuth(cfg *config) (string, error) {
	if cfg.Username != "" {
		credentials := fmt.Sprintf("%s:%s", cfg.Username, cfg.Password)
		encodedCredentials := base64.StdEncoding.EncodeToString([]byte(credentials))

		return fmt.Sprintf("Basic %s", encodedCredentials), nil
	}

	return "", fmt.Errorf("invalid config")
}

func getConnector(ctx context.Context, cfg *config) (types.ConnectorServer, error) {
	l := ctxzap.Extract(ctx)

	// compose the auth options
	auth, err := constructAuth(cfg)
	if err != nil {
		return nil, err
	}

	servicenowConnector, err := connector.New(ctx, auth, cfg.Deployment)
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
