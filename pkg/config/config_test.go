package config

import (
	"strings"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/field"
	"github.com/stretchr/testify/require"
)

const (
	mutexMsg      = "fields marked as mutually exclusive were set"
	dependencyMsg = "dependent on"
)

func validate(t *testing.T, cfg ServiceNow) error {
	t.Helper()
	cfg.Username, cfg.Password, cfg.Deployment = "u", "p", "dev"
	return field.Validate(Config, &cfg)
}

func TestTicketSchemaIDsSelectionValidation(t *testing.T) {
	ids := []string{"abc123"}

	tests := []struct {
		name    string
		cfg     ServiceNow
		wantErr string // substring; empty means valid
	}{
		// Valid combinations with ticketing on.
		{"no selection", ServiceNow{Ticketing: true}, ""},
		{"ids alone", ServiceNow{Ticketing: true, TicketSchemaIds: ids}, ""},
		{"catalog alone", ServiceNow{Ticketing: true, CatalogId: "c"}, ""},
		{"category alone", ServiceNow{Ticketing: true, CategoryId: "g"}, ""},
		{"catalog and category together", ServiceNow{Ticketing: true, CatalogId: "c", CategoryId: "g"}, ""},

		// Invalid pairs: IDs against each filter, and against both.
		{"ids and catalog", ServiceNow{Ticketing: true, TicketSchemaIds: ids, CatalogId: "c"}, mutexMsg},
		{"ids and category", ServiceNow{Ticketing: true, TicketSchemaIds: ids, CategoryId: "g"}, mutexMsg},
		{"ids, catalog and category", ServiceNow{Ticketing: true, TicketSchemaIds: ids, CatalogId: "c", CategoryId: "g"}, mutexMsg},

		// Empty vs blank: an empty slice is unset, but a slice holding a blank
		// or whitespace entry is a set field even though the connector later
		// normalizes it away.
		{"empty slice and catalog", ServiceNow{Ticketing: true, TicketSchemaIds: []string{}, CatalogId: "c"}, ""},
		{"nil slice and category", ServiceNow{Ticketing: true, TicketSchemaIds: nil, CategoryId: "g"}, ""},
		{"blank entry and catalog", ServiceNow{Ticketing: true, TicketSchemaIds: []string{""}, CatalogId: "c"}, mutexMsg},
		{"whitespace entry and category", ServiceNow{Ticketing: true, TicketSchemaIds: []string{" "}, CategoryId: "g"}, mutexMsg},
		{"id with trailing blank and catalog", ServiceNow{Ticketing: true, TicketSchemaIds: []string{"a", ""}, CatalogId: "c"}, mutexMsg},

		// Ticketing off: every selection field depends on ticketing. That
		// constraint is listed first, so it is the error reported.
		{"ticketing off, no selection", ServiceNow{}, ""},
		{"ticketing off, empty slice", ServiceNow{TicketSchemaIds: []string{}}, ""},
		{"ticketing off, ids", ServiceNow{TicketSchemaIds: ids}, dependencyMsg},
		{"ticketing off, catalog", ServiceNow{CatalogId: "c"}, dependencyMsg},
		{"ticketing off, category", ServiceNow{CategoryId: "g"}, dependencyMsg},
		{"ticketing off, ids and catalog", ServiceNow{TicketSchemaIds: ids, CatalogId: "c"}, dependencyMsg},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validate(t, tt.cfg)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

// The two exclusions must stay pairwise. A single three-field group would also
// reject catalog-id together with category-id, which is a valid combination.
func TestTicketSchemaIDsConstraintsArePairwise(t *testing.T) {
	var pairs [][]string
	for _, c := range Config.Constraints {
		if c.Kind != field.MutuallyExclusive {
			continue
		}
		var names []string
		for _, f := range c.Fields {
			names = append(names, f.FieldName)
		}
		pairs = append(pairs, names)
	}
	require.Equal(t, [][]string{
		{"ticket-schema-ids", "catalog-id"},
		{"ticket-schema-ids", "category-id"},
	}, pairs)
}

func TestTicketSchemaIDsConstraintsSerialized(t *testing.T) {
	b, err := Config.MarshalJSON()
	require.NoError(t, err)
	s := string(b)
	require.Equal(t, 2, strings.Count(s, "CONSTRAINT_KIND_MUTUALLY_EXCLUSIVE"))
	for _, name := range []string{"ticket-schema-ids", "catalog-id", "category-id"} {
		require.Contains(t, s, `"`+name+`"`)
	}
}
