![Baton Logo](./docs/images/baton-logo.png)

# `baton-servicenow` [![Go Reference](https://pkg.go.dev/badge/github.com/conductorone/baton-servicenow.svg)](https://pkg.go.dev/github.com/conductorone/baton-servicenow) ![main ci](https://github.com/conductorone/baton-servicenow/actions/workflows/main.yaml/badge.svg)

`baton-servicenow` is a connector for ServiceNow built using the [Baton SDK](https://github.com/conductorone/baton-sdk). It works with the ServiceNow Table API to sync data about users, groups and roles.

Check out [Baton](https://github.com/conductorone/baton) to learn more about the project in general.

# Prerequisites

To work with the connector, you have to have a running instance of ServiceNow. You can request a free developer instance [here](https://developer.servicenow.com/dev.do).

You can then use credentials to log in as credentials for communicating with API (username and password). 

Along with this, user represented by the credentials has to have either `admin` role or ACL (Access control list) set up for relevant tables to be able to read and modify tables in Table API.

By default, user without any roles have only restricted access to tables with users and groups. If you assign `admin` role to the user, ACLs for relevant tables are already set up and can use the connector without any additional configuration. If you don't want to use default `admin` role, you can configure ACLs for relevant tables manually. You can find more information about ACLs [here](https://docs.servicenow.com/bundle/utah-platform-security/page/administer/contextual-security/concept/access-control-rules.html).

### Relevant Tables:
- `sys_user` - Users
- `sys_user_role` - Roles
- `sys_user_group` - Groups
- `sys_user_grmember` - Group membership
- `sys_user_has_role` - User roles
- `sys_group_has_role` - Group roles

# Getting Started

Along with credentials, you have to provide also ID of the deployment you are using (under environment variable `BATON_DEPLOYMENT` or CLI flag `--deployment`). 

You can find it in the URL of your ServiceNow instance. For example, if your URL is `https://dev12345.service-now.com/`, your deployment ID is `dev12345`.

## brew

```
brew install conductorone/baton/baton conductorone/baton/baton-servicenow

BATON_USERNAME=username BATON_PASSWORD=password BATON_DEPLOYMENT=deployment baton-servicenow
baton resources
```

## docker

```
docker run --rm -v $(pwd):/out -e BATON_USERNAME=username BATON_PASSWORD=password BATON_DEPLOYMENT=deployment ghcr.io/conductorone/baton-servicenow:latest -f "/out/sync.c1z"
docker run --rm -v $(pwd):/out ghcr.io/conductorone/baton:latest -f "/out/sync.c1z" resources
```

## source

```
go install github.com/conductorone/baton/cmd/baton@main
go install github.com/conductorone/baton-servicenow/cmd/baton-servicenow@main

BATON_USERNAME=username BATON_PASSWORD=password BATON_DEPLOYMENT=deployment baton-servicenow
baton resources
```

# Data Model

`baton-servicenow` will fetch information about the following ServiceNow resources:

- Users
- Groups
- Roles

# Contributing, Support and Issues

We started Baton because we were tired of taking screenshots and manually building spreadsheets. We welcome contributions, and ideas, no matter how small -- our goal is to make identity and permissions sprawl less painful for everyone. If you have questions, problems, or ideas: Please open a Github Issue!

See [CONTRIBUTING.md](https://github.com/ConductorOne/baton/blob/main/CONTRIBUTING.md) for more details.

# `baton-servicenow` Command Line Usage

```
baton-servicenow

Usage:
  baton-servicenow [flags]
  baton-servicenow [command]

Available Commands:
  capabilities       Get connector capabilities
  completion         Generate the autocompletion script for the specified shell
  help               Help about any command

Flags:
      --catalog-id string      ServiceNow catalog id to filter catalog items to ($BATON_CATALOG_ID)
      --category-id string     ServiceNow category id to filter catalog items to ($BATON_CATEGORY_ID)
      --client-id string       The client ID used to authenticate with ConductorOne ($BATON_CLIENT_ID)
      --client-secret string   The client secret used to authenticate with ConductorOne ($BATON_CLIENT_SECRET)
      --deployment string      required: ServiceNow deployment to connect to. ($BATON_DEPLOYMENT)
  -f, --file string            The path to the c1z file to sync with ($BATON_FILE) (default "sync.c1z")
  -h, --help                   help for baton-servicenow
      --log-format string      The output format for logs: json, console ($BATON_LOG_FORMAT) (default "json")
      --log-level string       The log level: debug, info, warn, error ($BATON_LOG_LEVEL) (default "info")
      --password string        required: Application password used to connect to the ServiceNow API. ($BATON_PASSWORD)
  -p, --provisioning           This must be set in order for provisioning actions to be enabled ($BATON_PROVISIONING)
      --skip-full-sync         This must be set to skip a full sync ($BATON_SKIP_FULL_SYNC)
      --ticketing              This must be set to enable ticketing support ($BATON_TICKETING)
      --username string        required: Username of administrator used to connect to the ServiceNow API. ($BATON_USERNAME)
  -v, --version                version for baton-servicenow

Use "baton-servicenow [command] --help" for more information about a command.
```
