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
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/conductorone/baton-servicenow/pkg/servicenow"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// schemaStub is a ServiceNow stand-in for the ticket schema listing paths. Known
// items answer; any other ID gets the 400 ServiceNow returns for items that are
// not found, malformed, inactive or access-denied.
type schemaStub struct {
	mu sync.Mutex
	// known item IDs, in the order discovery would list them.
	items []string
	// failID, when set, makes GET of that item return a 500.
	failID string
	// itemStatus overrides the response to GET of an item: status -> body.
	itemStatus map[string]stubResponse
	// variablesStatus overrides the response to GET of an item's variables.
	variablesStatus map[string]stubResponse
	// dropID, when set, makes GET of that item close the connection without a response.
	dropID string
	// hangID, when set, makes GET of that item wait until the client gives up.
	hangID string
	// statesStatus, when nonzero, is returned for the requested-item states call.
	statesStatus int
	// calls records every request path+query, in arrival order.
	calls []string
}

type stubResponse struct {
	code int
	body string
}

// variablesDenied400Body is what the item variables endpoint returns for an
// unavailable item (observed live); it differs from the item endpoint's body.
const variablesDenied400Body = `{"error":{"message":"Security constraints prevent accessing of the item","detail":""},"status":"failure"}`

const vendor400Body = `{"error":{"message":"Either Catalog Item is not valid or security constraints prevent access","detail":""},"status":"failure"}`

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
			if s.statesStatus != 0 {
				w.WriteHeader(s.statesStatus)
				return
			}
			payload = servicenow.RequestedItemStateResponse{Result: []servicenow.RequestItemState{{Label: "Open", Value: "1"}, {Label: "Closed Complete", Value: "3"}}}
		case strings.HasSuffix(path, "/io_set_item"):
			payload = servicenow.VariableSetM2MResponse{}
		case strings.HasSuffix(path, "/variables"):
			_, rest, _ := strings.Cut(path, "/items/")
			id := strings.TrimSuffix(rest, "/variables")
			if resp, ok := s.variablesStatus[id]; ok {
				w.WriteHeader(resp.code)
				_, _ = w.Write([]byte(resp.body))
				return
			}
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
			_, id, _ := strings.Cut(path, "/items/")
			if id == s.dropID && id != "" {
				conn, _, err := w.(http.Hijacker).Hijack()
				if err != nil {
					t.Errorf("failed to hijack connection: %v", err)
					return
				}
				_ = conn.Close()
				return
			}
			if id == s.hangID && id != "" {
				<-r.Context().Done()
				return
			}
			if resp, ok := s.itemStatus[id]; ok {
				w.WriteHeader(resp.code)
				_, _ = w.Write([]byte(resp.body))
				return
			}
			if id == s.failID && id != "" {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			if !s.known(id) {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(vendor400Body))
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
	normalized := normalizeTicketSchemaIDs(ids)
	valid, _ := splitTicketSchemaIDs(normalized)
	return &ServiceNow{client: client, ticketSchemaIDs: valid, ticketSchemaIDsSelected: len(ids) > 0}
}

// capturedLogs is a minimal zapcore.Core that records entries (zaptest/observer
// is not vendored).
type capturedLogs struct {
	mu      sync.Mutex
	entries []capturedEntry
}

type capturedEntry struct {
	level  zapcore.Level
	msg    string
	fields map[string]any
}

func (c *capturedLogs) Enabled(zapcore.Level) bool        { return true }
func (c *capturedLogs) With([]zapcore.Field) zapcore.Core { return c }
func (c *capturedLogs) Check(e zapcore.Entry, ce *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	return ce.AddCore(e, c)
}
func (c *capturedLogs) Sync() error { return nil }
func (c *capturedLogs) Write(e zapcore.Entry, fields []zapcore.Field) error {
	enc := zapcore.NewMapObjectEncoder()
	for _, f := range fields {
		f.AddTo(enc)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries = append(c.entries, capturedEntry{level: e.Level, msg: e.Message, fields: enc.Fields})
	return nil
}

func (c *capturedLogs) warnings() []capturedEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []capturedEntry
	for _, e := range c.entries {
		if e.level == zapcore.WarnLevel && strings.HasPrefix(e.msg, "baton-servicenow:") {
			out = append(out, e)
		}
	}
	return out
}

func logCtx(logs *capturedLogs) context.Context {
	return ctxzap.ToContext(context.Background(), zap.New(logs))
}

// newViaNew builds the connector through New, as the CLI does, against the stub.
func newViaNew(t *testing.T, ctx context.Context, stub *schemaStub, filters map[string]string, ids []string) (*ServiceNow, error) {
	t.Helper()
	server := httptest.NewServer(stub.handler(t))
	t.Cleanup(server.Close)
	return New(ctx, "Basic dGVzdDp0ZXN0", "dev0", filters, ids, nil, nil, server.URL, false, false)
}

func listAll(t *testing.T, ctx context.Context, s *ServiceNow, size int) ([]string, int) {
	t.Helper()
	var got []string
	token := ""
	pages := 0
	for {
		schemas, next, _, err := s.ListTicketSchemas(ctx, &pagination.Token{Size: size, Token: token})
		require.NoError(t, err)
		for _, sc := range schemas {
			got = append(got, sc.GetId())
		}
		pages++
		if next == "" {
			return got, pages
		}
		require.Less(t, pages, 1000)
		token = next
	}
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
	s := newSchemaTestConnector(t, stub, map[string]string{"sysparm_catalog": "cat", "sysparm_category": "cat2"}, nil)

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
	require.Equal(t, "3:ok", next, "offset plus the flag that a schema was already produced")

	ids, next = schemaIDsOf(t, s, 3, next)
	require.Equal(t, all[3:6], ids)
	require.Equal(t, "6:ok", next)

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
		require.Equal(t, strconv.Itoa(TicketSchemasPageSize)+":ok", next, "size %d", size)
	}

	ids, next := schemaIDsOf(t, s, 10, "")
	require.Len(t, ids, 10)
	require.Equal(t, "10:ok", next)
}

func TestListTicketSchemas_ExplicitIDsInvalidPageTokens(t *testing.T) {
	all := makeIDs(5)
	stub := &schemaStub{items: all}
	s := newSchemaTestConnector(t, stub, nil, all)

	for _, token := range []string{"abc", "-1", "6", "1.5", "99999", "+1", " 1", ":ok", "ok", "1:OK", "1:ok:ok", "1:", "6:ok"} {
		_, _, _, err := s.ListTicketSchemas(context.Background(), &pagination.Token{Size: 25, Token: token})
		require.Error(t, err, "token %q", token)
	}

	// A token exactly at the end of the list is an empty final page, not an error.
	for _, token := range []string{"5", "5:ok"} {
		ids, next := schemaIDsOf(t, s, 25, token)
		require.Empty(t, ids)
		require.Empty(t, next)
	}
	require.Empty(t, stub.calls)
}

func TestSelectedPageToken(t *testing.T) {
	for _, tc := range []struct {
		offset    int
		succeeded bool
		token     string
	}{{0, false, "0"}, {25, false, "25"}, {25, true, "25:ok"}, {100, true, "100:ok"}} {
		require.Equal(t, tc.token, encodeSelectedPageToken(tc.offset, tc.succeeded))
		offset, succeeded, err := decodeSelectedPageToken(tc.token)
		require.NoError(t, err)
		require.Equal(t, tc.offset, offset)
		require.Equal(t, tc.succeeded, succeeded)
	}

	offset, succeeded, err := decodeSelectedPageToken("")
	require.NoError(t, err)
	require.Zero(t, offset)
	require.False(t, succeeded, "first page starts with no success")

	for _, bad := range []string{"abc", "-1", "+1", " 1", "1 ", "1.5", ":ok", "ok", "1:OK", "1:ok:ok", "1:", "1:no", "0x1", "99999999999999999999"} {
		_, _, err := decodeSelectedPageToken(bad)
		require.Error(t, err, "token %q", bad)
	}
}

// A selection at the maximum is traversed to its end by small pages.
func TestListTicketSchemas_ExplicitIDsAtMaximumTraverse(t *testing.T) {
	all := makeIDs(MaxTicketSchemaIDs)
	stub := &schemaStub{items: all}
	s := newSchemaTestConnector(t, stub, nil, all)

	got, pages := listAll(t, context.Background(), s, 0)
	require.Equal(t, all, got)
	require.Equal(t, 4, pages, "100 IDs at the default page size of 25")
	require.Zero(t, stub.count("/servicecatalog/items?"))
}

// More than the maximum does not fail construction (so Validate and resource
// sync still work); schema listing rejects it on every page, before any remote
// call, and never truncates.
func TestListTicketSchemas_RejectsMoreThanMaximumDistinctIDs(t *testing.T) {
	over := makeIDs(MaxTicketSchemaIDs + 1)
	stub := &schemaStub{items: over}

	s, err := newViaNew(t, context.Background(), stub, nil, over)
	require.NoError(t, err, "New must not fail on the count")
	require.NotEmpty(t, s.ResourceSyncers(context.Background()), "resource sync is still wired")
	require.Empty(t, stub.calls, "no remote call at construction")

	for _, token := range []string{"", "25", "25:ok", "100"} {
		schemas, next, _, err := s.ListTicketSchemas(context.Background(), &pagination.Token{Size: 25, Token: token})
		require.Error(t, err, "token %q: never truncated silently", token)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
		require.Contains(t, err.Error(), "101")
		require.Contains(t, err.Error(), "at most 100")
		require.Nil(t, schemas)
		require.Empty(t, next)
	}
	require.Empty(t, stub.calls, "no remote call is made to enforce the maximum")

	// Blanks, whitespace and duplicates do not count toward the maximum.
	padded := append(append([]string{" ", ""}, makeIDs(MaxTicketSchemaIDs)...), " id000 ", "id001")
	s, err = newViaNew(t, context.Background(), stub, nil, padded)
	require.NoError(t, err)
	require.Len(t, s.ticketSchemaIDs, MaxTicketSchemaIDs)
	got, pages := listAll(t, context.Background(), s, 0)
	require.Equal(t, makeIDs(MaxTicketSchemaIDs), got)
	require.Equal(t, 4, pages)

	// Malformed entries still count: the maximum applies before format filtering,
	// even though only 100 well-formed IDs remain.
	malformedStub := &schemaStub{items: over}
	malformed := append(makeIDs(MaxTicketSchemaIDs), "bad/id")
	s, err = newViaNew(t, context.Background(), malformedStub, nil, malformed)
	require.NoError(t, err)
	require.Len(t, s.ticketSchemaIDs, MaxTicketSchemaIDs)
	_, _, _, err = s.ListTicketSchemas(context.Background(), &pagination.Token{Size: 25})
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Empty(t, malformedStub.calls)
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

func TestListTicketSchemas_ItemSpecificFailuresAreSkippedWithWarning(t *testing.T) {
	cases := map[string]stubResponse{
		"vendor400": {http.StatusBadRequest, vendor400Body},
	}
	for name, resp := range cases {
		t.Run(name, func(t *testing.T) {
			stub := &schemaStub{items: []string{"good1", "good2"}, itemStatus: map[string]stubResponse{"bad": resp}}
			logs := &capturedLogs{}
			s, err := newViaNew(t, logCtx(logs), stub, nil, []string{"good1", "bad", "good2"})
			require.NoError(t, err)

			got, pages := listAll(t, logCtx(logs), s, 25)
			require.Equal(t, []string{"good1", "good2"}, got, "bad item skipped, listing continues")
			require.Equal(t, 1, pages)
			warns := logs.warnings()
			require.Len(t, warns, 1)
			require.Equal(t, "bad", warns[0].fields["ticket_schema_id"])
			require.NotEmpty(t, warns[0].fields["reason"])
		})
	}
}

func TestListTicketSchemas_SkipReasonsAreTruthful(t *testing.T) {
	stub := &schemaStub{items: []string{"good"}, itemStatus: map[string]stubResponse{"bad": {http.StatusBadRequest, vendor400Body}}}
	logs := &capturedLogs{}
	s := newSchemaTestConnector(t, stub, nil, []string{"bad", "good"})
	listAll(t, logCtx(logs), s, 25)
	reason := logs.warnings()[0].fields["reason"].(string)
	require.Contains(t, reason, "HTTP 400")
	require.NotContains(t, reason, "404")
}

func TestListTicketSchemas_OperationalFailuresAreNotSkipped(t *testing.T) {
	cases := map[string]struct {
		resp stubResponse
		code codes.Code
	}{
		"401":        {stubResponse{http.StatusUnauthorized, `{"error":{"message":"User Not Authenticated"}}`}, codes.Unauthenticated},
		"403":        {stubResponse{http.StatusForbidden, `{"error":{"message":"Insufficient rights"}}`}, codes.PermissionDenied},
		"429":        {stubResponse{http.StatusTooManyRequests, ``}, codes.Unavailable},
		"500":        {stubResponse{http.StatusInternalServerError, ``}, codes.Unavailable},
		"503":        {stubResponse{http.StatusServiceUnavailable, ``}, codes.Unavailable},
		"global 400": {stubResponse{http.StatusBadRequest, `{"error":{"message":"Invalid query parameter"}}`}, codes.InvalidArgument},
		// ServiceNow answers a missing route with 400 and this body, not 404.
		"missing route 400": {stubResponse{http.StatusBadRequest, `{"error":{"message":"Requested URI does not represent any resource","detail":null},"status":"failure"}`}, codes.InvalidArgument},
		"404":               {stubResponse{http.StatusNotFound, `{"error":{"message":"No Record found"}}`}, codes.NotFound},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			stub := &schemaStub{items: []string{"good1", "good2"}, itemStatus: map[string]stubResponse{"flaky": tc.resp}}
			s := newSchemaTestConnector(t, stub, nil, []string{"good1", "flaky", "good2"})

			schemas, _, annos, err := s.ListTicketSchemas(context.Background(), &pagination.Token{Size: 25})
			require.Error(t, err)
			require.Nil(t, schemas)
			require.Contains(t, err.Error(), `"flaky"`)
			require.NotContains(t, err.Error(), "not valid, inactive")
			require.Equal(t, tc.code, status.Code(err), "vendor status preserved")
			require.Zero(t, stub.count("/items/good2"), "stops at the operational failure")
			require.True(t, hasRateLimit(annos), "annotations from calls made so far are returned on the error path")
		})
	}

	// No HTTP response at all: never mistaken for an unavailable item.
	t.Run("transport error", func(t *testing.T) {
		stub := &schemaStub{items: []string{"good1", "good2"}, dropID: "flaky"}
		s := newSchemaTestConnector(t, stub, nil, []string{"good1", "flaky", "good2"})
		schemas, next, _, err := s.ListTicketSchemas(context.Background(), &pagination.Token{Size: 25})
		require.Error(t, err)
		require.Nil(t, schemas)
		require.Empty(t, next)
		require.Contains(t, err.Error(), `"flaky"`)
		require.Zero(t, stub.count("/items/good2"), "not skipped")
	})
	t.Run("client timeout", func(t *testing.T) {
		stub := &schemaStub{items: []string{"good1", "good2"}, hangID: "flaky"}
		s := newSchemaTestConnector(t, stub, nil, []string{"good1", "flaky", "good2"})
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		schemas, next, _, err := s.ListTicketSchemas(ctx, &pagination.Token{Size: 25})
		require.Error(t, err)
		require.Nil(t, schemas)
		require.Empty(t, next)
		require.Contains(t, err.Error(), `"flaky"`)
		require.Zero(t, stub.count("/items/good2"), "not skipped")
	})
}

// Schema building never calls the item endpoint, so no failure there is skipped,
// including the vendor body text and the variables endpoint's own 400.
func TestListTicketSchemas_SchemaBuildFailuresPropagate(t *testing.T) {
	for name, tc := range map[string]struct {
		resp stubResponse
		code codes.Code
		text string
	}{
		"variables endpoint 400":  {stubResponse{http.StatusBadRequest, variablesDenied400Body}, codes.InvalidArgument, "Security constraints prevent accessing of the item"},
		"item endpoint body text": {stubResponse{http.StatusBadRequest, vendor400Body}, codes.InvalidArgument, "Catalog Item is not valid"},
		"global 400":              {stubResponse{http.StatusBadRequest, `{"error":{"message":"bad request"}}`}, codes.InvalidArgument, "bad request"},
		"403":                     {stubResponse{http.StatusForbidden, ``}, codes.PermissionDenied, ""},
		"404":                     {stubResponse{http.StatusNotFound, ``}, codes.NotFound, ""},
		"500":                     {stubResponse{http.StatusInternalServerError, ``}, codes.Unknown, ""},
	} {
		t.Run(name, func(t *testing.T) {
			stub := &schemaStub{items: []string{"good", "x", "good2"}, variablesStatus: map[string]stubResponse{"x": tc.resp}}
			s := newSchemaTestConnector(t, stub, nil, []string{"good", "x", "good2"})
			schemas, next, annos, err := s.ListTicketSchemas(context.Background(), &pagination.Token{Size: 25})
			require.Error(t, err, "even though another configured ID produced a schema")
			require.Nil(t, schemas)
			require.Empty(t, next)
			require.Contains(t, err.Error(), "catalog item x")
			require.Contains(t, err.Error(), tc.text)
			if tc.code != codes.Unknown {
				require.Equal(t, tc.code, status.Code(err))
			}
			require.Zero(t, stub.count("/items/good2"))
			require.True(t, hasRateLimit(annos))
		})
	}
}

func TestListTicketSchemas_StatesFailureIsOperational(t *testing.T) {
	stub := &schemaStub{items: []string{"a"}, statesStatus: http.StatusServiceUnavailable}
	s := newSchemaTestConnector(t, stub, nil, []string{"a"})
	_, _, annos, err := s.ListTicketSchemas(context.Background(), &pagination.Token{Size: 25})
	require.Error(t, err)
	require.Equal(t, codes.Unavailable, status.Code(err))
	require.NotEmpty(t, annos)
}

// A page whose items were all skipped is empty but must still hand out the next
// token: pagination follows attempted positions, not successful results.
func TestListTicketSchemas_EmptyPageKeepsPaging(t *testing.T) {
	bad := map[string]stubResponse{}
	var ids []string
	for i := 0; i < 5; i++ {
		id := fmt.Sprintf("bad%d", i)
		bad[id] = stubResponse{http.StatusBadRequest, vendor400Body}
		ids = append(ids, id)
	}
	ids = append(ids, "good")
	stub := &schemaStub{items: []string{"good"}, itemStatus: bad}
	s := newSchemaTestConnector(t, stub, nil, ids)

	schemas, next, _, err := s.ListTicketSchemas(context.Background(), &pagination.Token{Size: 5})
	require.NoError(t, err)
	require.Empty(t, schemas)
	require.Equal(t, "5", next, "next page remains reachable after an all-skipped page")
	require.NotContains(t, next, selectedTokenSucceededSuffix, "no schema produced yet, so no success flag")

	got, _ := schemaIDsOf(t, s, 5, next)
	require.Equal(t, []string{"good"}, got)
}

func TestListTicketSchemas_AnnotationsKeepSingleMostRestrictiveRateLimit(t *testing.T) {
	stub := &schemaStub{items: []string{"a", "b", "c"}}
	s := newSchemaTestConnector(t, stub, nil, []string{"a", "b", "c"})
	_, _, annos, err := s.ListTicketSchemas(context.Background(), &pagination.Token{Size: 25})
	require.NoError(t, err)
	require.Len(t, stub.calls, 1+3*3, "states + (item, variables, set links) per item")
	require.Len(t, annos, 1, "a single-value annotation type appears once")
	require.True(t, annos[0].MessageIs(&v2.RateLimitDescription{}))
}

func TestMergeAnnotations(t *testing.T) {
	rl := func(status v2.RateLimitDescription_Status, remaining int64) *anypb.Any {
		a, err := anypb.New(v2.RateLimitDescription_builder{Status: status, Remaining: remaining}.Build())
		require.NoError(t, err)
		return a
	}
	other, err := anypb.New(&v2.GrantAlreadyExists{})
	require.NoError(t, err)
	remaining := func(a *anypb.Any) (v2.RateLimitDescription_Status, int64) {
		var d v2.RateLimitDescription
		require.NoError(t, a.UnmarshalTo(&d))
		return d.GetStatus(), d.GetRemaining()
	}

	var dst annotations.Annotations
	servicenow.MergeAnnotations(&dst, annotations.Annotations{rl(v2.RateLimitDescription_STATUS_OK, 50), other})
	servicenow.MergeAnnotations(&dst, annotations.Annotations{rl(v2.RateLimitDescription_STATUS_OVERLIMIT, 0)})
	servicenow.MergeAnnotations(&dst, annotations.Annotations{rl(v2.RateLimitDescription_STATUS_OK, 90)})
	require.Len(t, dst, 2, "one rate limit plus the other annotation, nothing dropped")
	st, _ := remaining(dst[0])
	require.Equal(t, v2.RateLimitDescription_STATUS_OVERLIMIT, st, "a later healthy call does not hide an earlier overlimit")

	var low annotations.Annotations
	servicenow.MergeAnnotations(&low, annotations.Annotations{rl(v2.RateLimitDescription_STATUS_OK, 50)})
	servicenow.MergeAnnotations(&low, annotations.Annotations{rl(v2.RateLimitDescription_STATUS_OK, 5)})
	servicenow.MergeAnnotations(&low, annotations.Annotations{rl(v2.RateLimitDescription_STATUS_OK, 80)})
	_, rem := remaining(low[0])
	require.Equal(t, int64(5), rem, "lowest remaining quota is kept")

	// What uhttp yields for a response with no rate-limit headers: unspecified,
	// zero limit/remaining and a zero-time ResetAt (not nil).
	noHeaders := func() *anypb.Any {
		a, err := anypb.New(v2.RateLimitDescription_builder{ResetAt: timestamppb.New(time.Time{})}.Build())
		require.NoError(t, err)
		return a
	}
	full := func(status v2.RateLimitDescription_Status, limit, remaining int64) *anypb.Any {
		a, err := anypb.New(v2.RateLimitDescription_builder{Status: status, Limit: limit, Remaining: remaining, ResetAt: timestamppb.New(time.Unix(1700000000, 0))}.Build())
		require.NoError(t, err)
		return a
	}
	merge := func(in ...*anypb.Any) *anypb.Any {
		var d annotations.Annotations
		for _, a := range in {
			servicenow.MergeAnnotations(&d, annotations.Annotations{a})
		}
		require.Len(t, d, 1)
		return d[0]
	}

	t.Run("empty incoming does not replace populated existing", func(t *testing.T) {
		st, rem := remaining(merge(rl(v2.RateLimitDescription_STATUS_OK, 50), noHeaders()))
		require.Equal(t, v2.RateLimitDescription_STATUS_OK, st)
		require.Equal(t, int64(50), rem)
	})
	t.Run("populated incoming replaces empty existing", func(t *testing.T) {
		st, rem := remaining(merge(noHeaders(), rl(v2.RateLimitDescription_STATUS_OK, 50)))
		require.Equal(t, v2.RateLimitDescription_STATUS_OK, st)
		require.Equal(t, int64(50), rem)
	})
	t.Run("two empty descriptions stay a single empty one", func(t *testing.T) {
		st, rem := remaining(merge(noHeaders(), noHeaders()))
		require.Equal(t, v2.RateLimitDescription_STATUS_UNSPECIFIED, st)
		require.Zero(t, rem)
	})
	t.Run("real zero quota is kept in both orders", func(t *testing.T) {
		// Parser output for limit=100, remaining=0 headers: status stays unspecified.
		for name, in := range map[string][]*anypb.Any{
			"after healthy":  {full(v2.RateLimitDescription_STATUS_OK, 100, 50), full(v2.RateLimitDescription_STATUS_UNSPECIFIED, 100, 0)},
			"before healthy": {full(v2.RateLimitDescription_STATUS_UNSPECIFIED, 100, 0), full(v2.RateLimitDescription_STATUS_OK, 100, 50)},
			"after empty":    {noHeaders(), full(v2.RateLimitDescription_STATUS_UNSPECIFIED, 100, 0)},
			"before empty":   {full(v2.RateLimitDescription_STATUS_UNSPECIFIED, 100, 0), noHeaders()},
		} {
			t.Run(name, func(t *testing.T) {
				_, rem := remaining(merge(in...))
				require.Zero(t, rem)
			})
		}
	})
	t.Run("no-header 429 overlimit survives an empty and a healthy response", func(t *testing.T) {
		// Parser output for a bare 429: overlimit, limit 1, remaining 0, reset in 60s.
		over := full(v2.RateLimitDescription_STATUS_OVERLIMIT, 1, 0)
		for name, in := range map[string][]*anypb.Any{
			"over then empty":   {over, noHeaders()},
			"empty then over":   {noHeaders(), over},
			"over then healthy": {over, rl(v2.RateLimitDescription_STATUS_OK, 90)},
			"healthy then over": {rl(v2.RateLimitDescription_STATUS_OK, 90), over},
		} {
			t.Run(name, func(t *testing.T) {
				st, _ := remaining(merge(in...))
				require.Equal(t, v2.RateLimitDescription_STATUS_OVERLIMIT, st)
			})
		}
	})
}

func TestNew_MalformedIDsWarnOnceAndAreOmitted(t *testing.T) {
	for _, bad := range []string{"../variables", "abc/def", "abc?x=1", "..", "a%2Fb", "a#b", "a b", "a\\b", "a\nb"} {
		t.Run(bad, func(t *testing.T) {
			stub := &schemaStub{items: []string{"good1"}}
			logs := &capturedLogs{}
			s, err := newViaNew(t, logCtx(logs), stub, nil, []string{"good1", bad})
			require.NoError(t, err, "a malformed ID must not fail construction")
			require.Len(t, logs.warnings(), 1)
			require.NotContains(t, logs.warnings()[0].fields["ticket_schema_id"], "\n")

			got, _ := schemaIDsOf(t, s, 25, "")
			require.Equal(t, []string{"good1"}, got)
			require.Zero(t, stub.count(strings.TrimSpace(bad)+"?"), "malformed ID never reaches the URL")
			require.Len(t, logs.warnings(), 1, "listing adds no repeated warning")
		})
	}

	t.Run("sys_id shaped IDs are accepted", func(t *testing.T) {
		ids := []string{"79c99042b7321010e54deb56ee11a907", "MixedCase09", "with-hyphen_underscore"}
		stub := &schemaStub{items: ids}
		s := newSchemaTestConnector(t, stub, nil, ids)
		got, _ := schemaIDsOf(t, s, 25, "")
		require.Equal(t, ids, got)
	})
}

func TestNew_LongMalformedIDIsTruncatedInLog(t *testing.T) {
	logs := &capturedLogs{}
	_, err := newViaNew(t, logCtx(logs), &schemaStub{}, nil, []string{strings.Repeat("x/", 200)})
	require.NoError(t, err)
	logged := logs.warnings()[0].fields["ticket_schema_id"].(string)
	require.LessOrEqual(t, len(logged), maxLoggedIDLength+3)
}

// Configured but entirely unusable must offer nothing, never fall back to
// discovery, and warn that the whole selection is unusable.
func TestListTicketSchemas_AllMalformedReturnsZeroSchemasWithoutDiscovery(t *testing.T) {
	stub := &schemaStub{items: []string{"disc1", "disc2"}}
	logs := &capturedLogs{}
	s, err := newViaNew(t, logCtx(logs), stub, map[string]string{"sysparm_catalog": "cat"}, []string{"a/b", "c?d"})
	require.NoError(t, err)

	schemas, next, _, err := s.ListTicketSchemas(context.Background(), &pagination.Token{Size: 25})
	require.NoError(t, err)
	require.Empty(t, schemas)
	require.Empty(t, next)
	require.Empty(t, stub.calls, "no discovery and no item calls")
	var summary bool
	for _, w := range logs.warnings() {
		if strings.Contains(w.msg, "none of the configured ticket schema IDs is usable") {
			summary = true
		}
	}
	require.True(t, summary, "summary warning when every ID is unusable")
}

// An explicit selection is decided by what was provided, before normalization,
// matching config validation (a blank entry counts as set; nil/empty is unset).
func TestNew_BlankOnlySelectionIsExplicitAndNeverFallsBackToDiscovery(t *testing.T) {
	for name, ids := range map[string][]string{
		"empty string":  {""},
		"whitespace":    {"  ", "\t"},
		"mixed blanks":  {"", " ", ""},
		"blank + dupes": {" ", " "},
	} {
		t.Run(name, func(t *testing.T) {
			stub := &schemaStub{items: []string{"disc1", "disc2"}}
			s, err := newViaNew(t, context.Background(), stub, map[string]string{"sysparm_catalog": "cat"}, ids)
			require.NoError(t, err)

			schemas, next, _, err := s.ListTicketSchemas(context.Background(), &pagination.Token{Size: 25})
			require.NoError(t, err)
			require.Empty(t, schemas)
			require.Empty(t, next)
			require.Empty(t, stub.calls, "no discovery, states or item calls")
		})
	}
}

func TestNew_BlanksMixedWithValidIDsListOnlyTheValidOnes(t *testing.T) {
	stub := &schemaStub{items: []string{"a", "b", "disc"}}
	s, err := newViaNew(t, context.Background(), stub, nil, []string{"", " a ", "  ", "b", ""})
	require.NoError(t, err)
	got, _ := schemaIDsOf(t, s, 25, "")
	require.Equal(t, []string{"a", "b"}, got)
	require.Zero(t, stub.count("/servicecatalog/items?"), "no discovery")
	require.Zero(t, stub.count("/items/disc"))
}

func TestNew_UnsetSelectionKeepsDiscovery(t *testing.T) {
	for name, ids := range map[string][]string{"nil": nil, "empty": {}} {
		t.Run(name, func(t *testing.T) {
			stub := &schemaStub{items: []string{"disc1", "disc2"}}
			s, err := newViaNew(t, context.Background(), stub, nil, ids)
			require.NoError(t, err)
			require.False(t, s.ticketSchemaIDsSelected)
			got, _ := schemaIDsOf(t, s, 25, "")
			require.Equal(t, []string{"disc1", "disc2"}, got)
			require.Positive(t, stub.count("/servicecatalog/items?"))
		})
	}
}

func hasRateLimit(annos annotations.Annotations) bool {
	for _, a := range annos {
		if a.MessageIs(&v2.RateLimitDescription{}) {
			return true
		}
	}
	return false
}

// When no configured ID yields a schema, the recognized 400 is not tolerated:
// listing fails with the original vendor error rather than an empty success.
func TestListTicketSchemas_AllRecognized400OnSinglePageFails(t *testing.T) {
	stub := &schemaStub{}
	s := newSchemaTestConnector(t, stub, nil, []string{"a", "b", "c"})
	schemas, next, annos, err := s.ListTicketSchemas(context.Background(), &pagination.Token{Size: 25})
	require.Error(t, err)
	require.Nil(t, schemas)
	require.Empty(t, next)
	require.Equal(t, codes.InvalidArgument, status.Code(err), "original vendor status survives wrapping")
	require.Contains(t, err.Error(), "Either Catalog Item is not valid or security constraints prevent access")
	require.Contains(t, err.Error(), "none of the 3 configured ticket schema IDs")
	require.Contains(t, err.Error(), `"a"`, "names the first skipped ID")
	require.True(t, hasRateLimit(annos))
	require.Equal(t, 3, stub.count("/servicecatalog/items/"))
}

func TestListTicketSchemas_AllRecognized400AcrossPagesFailsOnFinalPage(t *testing.T) {
	stub := &schemaStub{}
	logs := &capturedLogs{}
	s := newSchemaTestConnector(t, stub, nil, makeIDs(5))

	schemas, next, _, err := s.ListTicketSchemas(logCtx(logs), &pagination.Token{Size: 2})
	require.NoError(t, err, "a non-final page never fails just because all its IDs were skipped")
	require.Empty(t, schemas)
	require.Equal(t, "2", next)

	schemas, next, _, err = s.ListTicketSchemas(logCtx(logs), &pagination.Token{Size: 2, Token: next})
	require.NoError(t, err)
	require.Empty(t, schemas)
	require.Equal(t, "4", next)

	schemas, next, annos, err := s.ListTicketSchemas(logCtx(logs), &pagination.Token{Size: 2, Token: next})
	require.Error(t, err)
	require.Nil(t, schemas)
	require.Empty(t, next)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Contains(t, err.Error(), "none of the 5 configured ticket schema IDs")
	require.Contains(t, err.Error(), `"id004"`)
	require.True(t, hasRateLimit(annos))

	var allSkipped int
	for _, w := range logs.warnings() {
		if strings.Contains(w.msg, "every configured ticket schema ID on this page was skipped") {
			allSkipped++
			require.Equal(t, false, w.fields["last_page"])
		}
	}
	require.Equal(t, 2, allSkipped, "summary warns on the non-final pages; the failing page reports through its error")
}

func TestListTicketSchemas_HybridAcrossPageBoundaries(t *testing.T) {
	t.Run("first page all bad, later page good", func(t *testing.T) {
		stub := &schemaStub{items: []string{"good1", "good2"}}
		s := newSchemaTestConnector(t, stub, nil, []string{"bad1", "bad2", "good1", "bad3", "good2"})

		ids, next := schemaIDsOf(t, s, 2, "")
		require.Empty(t, ids)
		require.Equal(t, "2", next)
		ids, next = schemaIDsOf(t, s, 2, next)
		require.Equal(t, []string{"good1"}, ids)
		require.Equal(t, "4:ok", next)
		ids, next = schemaIDsOf(t, s, 2, next)
		require.Equal(t, []string{"good2"}, ids)
		require.Empty(t, next)
	})
	t.Run("first page good, later pages including the final one all bad", func(t *testing.T) {
		stub := &schemaStub{items: []string{"good1", "good2"}}
		s := newSchemaTestConnector(t, stub, nil, []string{"good1", "good2", "bad1", "bad2", "bad3", "bad4"})
		got, pages := listAll(t, context.Background(), s, 2)
		require.Equal(t, []string{"good1", "good2"}, got)
		require.Equal(t, 3, pages)
	})
	t.Run("final all-bad page trusts the token flag, with no state kept between calls", func(t *testing.T) {
		stub := &schemaStub{items: []string{"good1"}}
		// A fresh connector, as after a process restart, given a token from an
		// earlier page that produced a schema.
		s := newSchemaTestConnector(t, stub, nil, []string{"good1", "bad1", "bad2"})
		got, next := schemaIDsOf(t, s, 1, "1:ok")
		require.Empty(t, got)
		require.Equal(t, "2:ok", next)
		got, next = schemaIDsOf(t, s, 1, next)
		require.Empty(t, got)
		require.Empty(t, next)
		require.Zero(t, stub.count("/items/good1"), "no extra vendor request to re-check earlier pages")

		// The same final page without the flag fails.
		_, _, _, err := s.ListTicketSchemas(context.Background(), &pagination.Token{Size: 1, Token: "2"})
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})
	t.Run("good and bad within one page", func(t *testing.T) {
		stub := &schemaStub{items: []string{"good1", "good2"}}
		s := newSchemaTestConnector(t, stub, nil, []string{"bad1", "good1", "bad2", "good2", "bad3"})
		got, pages := listAll(t, context.Background(), s, 25)
		require.Equal(t, []string{"good1", "good2"}, got)
		require.Equal(t, 1, pages)
	})
}

const skipMsg = "baton-servicenow: skipping configured ticket schema ID"

// skipEntries returns the per-ID skip entries at the given level.
func (c *capturedLogs) skipEntries(level zapcore.Level) []capturedEntry {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []capturedEntry
	for _, e := range c.entries {
		if e.level == level && e.msg == skipMsg {
			out = append(out, e)
		}
	}
	return out
}

func TestListTicketSchemas_SkipWarningsAreSampledPerPage(t *testing.T) {
	// 12 unusable IDs interleaved with good ones, all on one page.
	var ids, good []string
	stub := &schemaStub{variablesStatus: map[string]stubResponse{}}
	for i := 0; i < 12; i++ {
		ids = append(ids, fmt.Sprintf("bad%02d", i))
		g := fmt.Sprintf("good%02d", i)
		ids = append(ids, g)
		good = append(good, g)
		stub.items = append(stub.items, g)
	}
	ids = append(ids, "bad12") // 13 skips in total, one page of 25.

	logs := &capturedLogs{}
	s := newSchemaTestConnector(t, stub, nil, ids)
	got, pages := listAll(t, logCtx(logs), s, 25)
	require.Equal(t, 1, pages)
	require.Equal(t, good, got, "sampling must not change which schemas are returned")

	warns := logs.skipEntries(zapcore.WarnLevel)
	require.Len(t, warns, 2, "Warn only at occurrences 1 and 10")
	require.EqualValues(t, 1, warns[0].fields["total_occurrences"])
	require.Equal(t, "bad00", warns[0].fields["ticket_schema_id"])
	require.EqualValues(t, 10, warns[1].fields["total_occurrences"])
	require.Equal(t, "bad09", warns[1].fields["ticket_schema_id"])

	debugs := logs.skipEntries(zapcore.DebugLevel)
	require.Len(t, debugs, 11, "every other skip stays visible at debug with its ID")
	require.EqualValues(t, 13, debugs[len(debugs)-1].fields["total_occurrences"])
	require.Equal(t, "bad12", debugs[len(debugs)-1].fields["ticket_schema_id"])

	var summary *capturedEntry
	for _, w := range logs.warnings() {
		if strings.Contains(w.msg, "some configured ticket schema IDs on this page were skipped") {
			w := w
			summary = &w
		}
	}
	require.NotNil(t, summary, "sampled page reports the true skip count")
	require.EqualValues(t, 13, summary.fields["skipped"])
	require.EqualValues(t, 12, summary.fields["listed"])
}

func TestListTicketSchemas_SingleSkipAmongSuccessesWarnsWithoutSummary(t *testing.T) {
	stub := &schemaStub{items: []string{"a", "b"}}
	logs := &capturedLogs{}
	s := newSchemaTestConnector(t, stub, nil, []string{"a", "gone", "b"})
	got, _ := listAll(t, logCtx(logs), s, 25)
	require.Equal(t, []string{"a", "b"}, got)
	warns := logs.skipEntries(zapcore.WarnLevel)
	require.Len(t, warns, 1)
	require.EqualValues(t, 1, warns[0].fields["total_occurrences"])
	require.Empty(t, logs.skipEntries(zapcore.DebugLevel))
	require.Len(t, logs.warnings(), 1, "no extra summary when nothing was suppressed")
}

func TestListTicketSchemas_SkipSamplingRestartsEachPage(t *testing.T) {
	stub := &schemaStub{items: []string{"x5"}}
	logs := &capturedLogs{}
	// Two pages of 2 unusable IDs each, then a usable one: each page's first skip warns.
	s := newSchemaTestConnector(t, stub, nil, []string{"x1", "x2", "x3", "x4", "x5"})
	listAll(t, logCtx(logs), s, 2)
	warns := logs.skipEntries(zapcore.WarnLevel)
	require.Len(t, warns, 2)
	require.Equal(t, "x1", warns[0].fields["ticket_schema_id"])
	require.Equal(t, "x3", warns[1].fields["ticket_schema_id"])
	for _, w := range warns {
		require.EqualValues(t, 1, w.fields["total_occurrences"])
	}
	require.Len(t, logs.skipEntries(zapcore.DebugLevel), 2)
}

func TestListTicketSchemas_AllSkippedKeepsSamplingAndSummaryWarnings(t *testing.T) {
	// A non-final page of 11 unusable IDs, then a usable one.
	stub := &schemaStub{items: []string{"good"}}
	logs := &capturedLogs{}
	s := newSchemaTestConnector(t, stub, nil, append(makeIDs(11), "good"))
	got, _ := listAll(t, logCtx(logs), s, 11)
	require.Equal(t, []string{"good"}, got)
	require.Len(t, logs.skipEntries(zapcore.WarnLevel), 2)
	var all bool
	for _, w := range logs.warnings() {
		if strings.Contains(w.msg, "every configured ticket schema ID on this page was skipped") {
			all = true
			require.EqualValues(t, 11, w.fields["skipped"])
		}
	}
	require.True(t, all)
}
