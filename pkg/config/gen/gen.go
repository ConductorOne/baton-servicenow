package main

import (
	"github.com/conductorone/baton-sdk/pkg/config"
	cfg "github.com/conductorone/baton-servicenow/pkg/config"
)

func main() {
	config.Generate("service-now", cfg.Config)
}
