package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/conductorone/baton-servicenow/pkg/servicenow"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// schemaStub is a ServiceNow stand-in for the ticket schema listing paths. Known
// items answer; any other ID gets the 400 ServiceNow returns for items that are
// not found, malformed, inactive or access-denied.
type schemaStub struct {
	mu sync.Mutex
	// known item IDs, in the order discovery would list them.
	items []string
	// calls records every request path+query, in arrival order.
	calls []string
}

func (s *schemaStub) record(r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, r.URL.Path+"?"+r.URL.RawQuery)
}

func (s *schemaStub) count(substr string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.calls {
		if strings.Contains(c, substr) {
			n++
		}
	}
	return n
}

func (s *schemaStub) known(id string) bool {
	for _, i := range s.items {
		if i == id {
			return true
		}
	}
	return false
}

func (s *schemaStub) handler(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.record(r)
		w.Header().Set("Content-Type", "application/json")

		path := r.URL.Path
		var payload any
		switch {
		case strings.HasSuffix(path, "/sys_choice"):
			payload = servicenow.RequestedItemStateResponse{Result: []servicenow.RequestItemState{{Label: "Open", Value: "1"}, {Label: "Closed Complete", Value: "3"}}}
		case strings.HasSuffix(path, "/io_set_item"):
			payload = servicenow.VariableSetM2MResponse{}
		case strings.HasSuffix(path, "/variables"):
			id := strings.TrimSuffix(strings.TrimPrefix(path[strings.Index(path, "/items/"):], "/items/"), "/variables")
			payload = servicenow.CatalogItemVariablesResponse{Result: []servicenow.CatalogItemVariable{{
				ID: "var-" + id, Name: "reason", Label: "Reason", Active: true, Type: servicenow.VariableTypeNew(servicenow.TypeSingleLineText), Mandatory: true,
			}}}
		case strings.HasSuffix(path, "/servicecatalog/items"):
			result := make([]servicenow.CatalogItem, 0, len(s.items))
			for _, id := range s.items {
				result = append(result, servicenow.CatalogItem{BaseResource: servicenow.BaseResource{Id: id}, Name: "item " + id})
			}
			payload = servicenow.CatalogItemsResponse{Result: result}
		case strings.Contains(path, "/servicecatalog/items/"):
			id := path[strings.Index(path, "/items/")+len("/items/"):]
			if !s.known(id) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error":{"message":"Either Catalog Item is not valid or security constraints prevent access","detail":""},"status":"failure"}`))
				return
			}
			payload = servicenow.CatalogItemResponse{Result: servicenow.CatalogItem{BaseResource: servicenow.BaseResource{Id: id}, Name: "item " + id}}
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL)
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err := json.NewEncoder(w).Encode(payload); err != nil {
			t.Errorf("failed to encode test response: %v", err)
		}
	}
}

func newSchemaTestConnector(t *testing.T, stub *schemaStub, filters map[string]string, ids []string) *ServiceNow {
	t.Helper()
	server := httptest.NewServer(stub.handler(t))
	t.Cleanup(server.Close)

	client, err := servicenow.NewClient(uhttp.NewBaseHttpClient(server.Client()), "Basic dGVzdDp0ZXN0", "dev0", filters, nil, nil, server.URL)
	require.NoError(t, err)
	return &ServiceNow{client: client, ticketSchemaIDs: normalizeTicketSchemaIDs(ids)}
}

func makeIDs(n int) []string {
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("id%03d", i)
	}
	return ids
}

func schemaIDsOf(t *testing.T, s *ServiceNow, size int, token string) ([]string, string) {
	t.Helper()
	schemas, next, _, err := s.ListTicketSchemas(context.Background(), &pagination.Token{Size: size, Token: token})
	require.NoError(t, err)
	ids := make([]string, 0, len(schemas))
	for _, sc := range schemas {
		ids = append(ids, sc.GetId())
	}
	return ids, next
}

func TestNormalizeTicketSchemaIDs(t *testing.T) {
	require.Empty(t, normalizeTicketSchemaIDs(nil))
	require.Empty(t, normalizeTicketSchemaIDs([]string{"", "  ", "\t"}))
	require.Equal(t,
		[]string{"b", "a", "c"},
		normalizeTicketSchemaIDs([]string{" b", "a ", "", "b", "c", "a", "  c  "}),
		"trim, drop blanks, dedupe keeping first occurrence and configured order",
	)
}

func TestListTicketSchemas_ExplicitIDsAreExclusiveAndIgnoreFilters(t *testing.T) {
	stub := &schemaStub{items: []string{"disc1", "disc2", "sel1", "sel2"}}
	s := newSchemaTestConnector(t, stub,
		map[string]string{"sysparm_catalog": "cat", "sysparm_category": "cat2"},
		[]string{"sel2", "sel1"},
	)

	ids, next := schemaIDsOf(t, s, 25, "")
	require.Equal(t, []string{"sel2", "sel1"}, ids, "only selected schemas, in configured order, no discovery filler")
	require.Empty(t, next)
	require.Zero(t, stub.count("/servicecatalog/items?"), "discovery listing must not be called in explicit mode")
	require.Zero(t, stub.count("sysparm_catalog"))
	require.Zero(t, stub.count("sysparm_category"))
}

func TestListTicketSchemas_EmptyIDsKeepDiscoveryWithFilters(t *testing.T) {
	stub := &schemaStub{items: []string{"disc1", "disc2"}}
	s := newSchemaTestConnector(t, stub, map[string]string{"sysparm_catalog": "cat", "sysparm_category": "cat2"}, []string{" ", ""})

	ids, _ := schemaIDsOf(t, s, 25, "")
	require.Equal(t, []string{"disc1", "disc2"}, ids)
	require.Equal(t, 1, stub.count("/servicecatalog/items?"))
	require.Equal(t, 1, stub.count("sysparm_catalog=cat"))
	require.Equal(t, 1, stub.count("sysparm_category=cat2"))
}

func TestListTicketSchemas_ExplicitIDsDedupeAndKeepFullSchemas(t *testing.T) {
	stub := &schemaStub{items: []string{"a", "b", "c"}}
	s := newSchemaTestConnector(t, stub, nil, []string{" c ", "a", "c", "", "a"})

	schemas, next, _, err := s.ListTicketSchemas(context.Background(), &pagination.Token{Size: 25})
	require.NoError(t, err)
	require.Empty(t, next)
	require.Len(t, schemas, 2)
	require.Equal(t, "c", schemas[0].GetId())
	require.Equal(t, "a", schemas[1].GetId())
	for _, sc := range schemas {
		require.Equal(t, "item "+sc.GetId(), sc.GetDisplayName())
		require.Contains(t, sc.GetCustomFields(), "reason", "variables are mapped into the schema")
		require.Len(t, sc.GetStatuses(), 2)
		require.Equal(t, "Open", sc.GetStatuses()[0].GetDisplayName())
	}
	require.Equal(t, 1, stub.count("/sys_choice"), "statuses are fetched once per page, not per item")
	require.Equal(t, 1, stub.count("/items/c?"))
	require.Equal(t, 1, stub.count("/items/a?"), "duplicate IDs are fetched once")
}

func TestListTicketSchemas_ExplicitIDsPaging(t *testing.T) {
	all := makeIDs(7)
	stub := &schemaStub{items: all}
	s := newSchemaTestConnector(t, stub, nil, all)

	ids, next := schemaIDsOf(t, s, 3, "")
	require.Equal(t, all[0:3], ids)
	require.Equal(t, "3", next)

	ids, next = schemaIDsOf(t, s, 3, next)
	require.Equal(t, all[3:6], ids)
	require.Equal(t, "6", next)

	ids, next = schemaIDsOf(t, s, 3, next)
	require.Equal(t, all[6:], ids)
	require.Empty(t, next, "last page ends traversal")
}

func TestListTicketSchemas_ExplicitIDsDefaultAndCappedPageSizes(t *testing.T) {
	all := makeIDs(60)
	stub := &schemaStub{items: all}
	s := newSchemaTestConnector(t, stub, nil, all)

	for _, size := range []int{0, -1, TicketSchemasPageSize, 100, 1000} {
		ids, next := schemaIDsOf(t, s, size, "")
		require.Len(t, ids, TicketSchemasPageSize, "size %d", size)
		require.Equal(t, strconv.Itoa(TicketSchemasPageSize), next, "size %d", size)
	}

	ids, next := schemaIDsOf(t, s, 10, "")
	require.Len(t, ids, 10)
	require.Equal(t, "10", next)
}

func TestListTicketSchemas_ExplicitIDsInvalidPageTokens(t *testing.T) {
	all := makeIDs(5)
	stub := &schemaStub{items: all}
	s := newSchemaTestConnector(t, stub, nil, all)

	for _, token := range []string{"abc", "-1", "6", "1.5", "99999"} {
		_, _, _, err := s.ListTicketSchemas(context.Background(), &pagination.Token{Size: 25, Token: token})
		require.Error(t, err, "token %q", token)
	}

	// A token exactly at the end of the list is an empty final page, not an error.
	ids, next := schemaIDsOf(t, s, 25, "5")
	require.Empty(t, ids)
	require.Empty(t, next)
}

// The configured list is not capped by the connector: IDs past position 100 are
// fetched directly and a list longer than 100 is traversed to its end.
func TestListTicketSchemas_ExplicitIDsBeyondOneHundred(t *testing.T) {
	const total = 130
	all := makeIDs(total)
	stub := &schemaStub{items: all}
	s := newSchemaTestConnector(t, stub, nil, all)

	var got []string
	token := ""
	pages := 0
	for {
		ids, next := schemaIDsOf(t, s, 0, token)
		got = append(got, ids...)
		pages++
		if next == "" {
			break
		}
		require.Less(t, pages, total, "traversal must terminate")
		token = next
	}
	require.Equal(t, all, got, "all %d configured schemas are reachable in order", total)
	require.Equal(t, 6, pages, "130 IDs at the default page size of 25")
	require.Zero(t, stub.count("/servicecatalog/items?"))
}

func TestListTicketSchemas_ExplicitIDOutsideDiscoveryFirstHundred(t *testing.T) {
	// Item 120 is past where discovery (and the SDK cutoff) would reach.
	all := makeIDs(145)
	stub := &schemaStub{items: all}
	s := newSchemaTestConnector(t, stub, nil, []string{all[120], all[3]})

	ids, next := schemaIDsOf(t, s, 25, "")
	require.Equal(t, []string{all[120], all[3]}, ids)
	require.Empty(t, next)
}

func TestListTicketSchemas_ExplicitInvalidIDFailsAndNamesID(t *testing.T) {
	stub := &schemaStub{items: []string{"good1", "good2"}}
	s := newSchemaTestConnector(t, stub, nil, []string{"good1", "missing-or-inactive", "good2"})

	schemas, next, _, err := s.ListTicketSchemas(context.Background(), &pagination.Token{Size: 25})
	require.Error(t, err, "a requested invalid/inaccessible ID is never silently skipped")
	require.Nil(t, schemas)
	require.Empty(t, next)
	require.Contains(t, err.Error(), `"missing-or-inactive"`)
	require.Contains(t, err.Error(), "not accessible")
	require.Equal(t, codes.InvalidArgument, status.Code(err), "underlying vendor status is preserved")
}

func TestListTicketSchemas_ExplicitInvalidIDOnLaterPageFails(t *testing.T) {
	all := makeIDs(4)
	stub := &schemaStub{items: all}
	s := newSchemaTestConnector(t, stub, nil, append(append([]string{}, all...), "bad"))

	ids, next := schemaIDsOf(t, s, 4, "")
	require.Equal(t, all, ids)
	require.Equal(t, "4", next)

	_, _, _, err := s.ListTicketSchemas(context.Background(), &pagination.Token{Size: 4, Token: next})
	require.Error(t, err)
	require.Contains(t, err.Error(), `"bad"`)
}
