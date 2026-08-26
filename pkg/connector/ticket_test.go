package connector

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/uhttp"
	"github.com/conductorone/baton-servicenow/pkg/servicenow"
)

const (
	testRequestedItemID     = "89b745e747fe07102073e55a516d435d"
	testRequestedItemNumber = "RITM0012345"
)

// newCreateTicketTestClient stands up a ServiceNow stub that walks the
// CreateTicket call chain: order the catalog item, look up the request item the
// order produced, PATCH its description, then read its labels back during ticket
// conversion. Every step after the order runs against a request item that already
// exists in ServiceNow, so each one is a chance to lose its id.
type createTicketStub struct {
	// Response status for the description PATCH and the label lookup. Zero
	// means 200; these are the calls that fail in production.
	patchStatus int
	labelStatus int
}

func (c createTicketStub) failed(status int) bool {
	return status != 0 && status != http.StatusOK
}

func newCreateTicketTestClient(t *testing.T, stub createTicketStub) (*ServiceNow, func()) {
	t.Helper()

	requestedItem := servicenow.RequestedItem{
		BaseResource: servicenow.BaseResource{Id: testRequestedItemID},
		Number:       testRequestedItemNumber,
		State:        "1",
		Description:  "description as created",
		SysCreatedOn: "2026-08-25 08:15:58",
		SysUpdatedOn: "2026-08-25 08:15:58",
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		var payload any
		switch {
		case strings.HasSuffix(r.URL.Path, "/order_now"):
			payload = servicenow.OrderCatalogItemResponse{
				Result: servicenow.RequestInfo{RequestID: "REQ0012345", RequestNumber: "REQ0012345"},
			}
		case r.Method == http.MethodPatch:
			if stub.failed(stub.patchStatus) {
				w.WriteHeader(stub.patchStatus)
				return
			}
			updated := requestedItem
			updated.Description = "description after update"
			updated.SysUpdatedOn = "2026-08-25 08:16:58"
			payload = servicenow.RequestItemResponse{Result: updated}
		case strings.HasSuffix(r.URL.Path, "/sc_req_item"):
			payload = servicenow.RequestItemsResponse{Result: []servicenow.RequestedItem{requestedItem}}
		case strings.HasSuffix(r.URL.Path, "/label_entry"):
			if stub.failed(stub.labelStatus) {
				w.WriteHeader(stub.labelStatus)
				return
			}
			payload = servicenow.LabelEntriesLabelNameResponse{Result: []servicenow.LabelEntryName{}}
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
			return
		}

		if err := json.NewEncoder(w).Encode(payload); err != nil {
			// t.Fatalf must not be called from the handler's goroutine --
			// it would only stop this goroutine mid-response and surface a
			// confusing secondary failure in the test itself.
			t.Errorf("failed to encode test response: %v", err)
		}
	}))

	client, err := servicenow.NewClient(uhttp.NewBaseHttpClient(server.Client()), "Basic dGVzdDp0ZXN0", "dev0", nil, nil, nil, server.URL)
	if err != nil {
		server.Close()
		t.Fatalf("unexpected error creating client: %v", err)
	}

	return &ServiceNow{client: client}, server.Close
}

func newTestTicket() (*v2.Ticket, *v2.TicketSchema) {
	return &v2.Ticket{
			DisplayName: "Access request",
			Description: "please grant access",
		}, &v2.TicketSchema{
			Id:          "catalog-item-1",
			DisplayName: "Access request",
		}
}

// A failed description update must not lose the request item the order already
// created in ServiceNow. Returning a nil ticket here is worse than it looks: the
// item exists downstream, and C1 only records the external ticket id when the
// response carries a ticket -- without it the task is stranded mid-create with
// nothing left to reconcile it against.
func TestCreateTicket_KeepsCreatedItemWhenDescriptionUpdateFails(t *testing.T) {
	s, closeServer := newCreateTicketTestClient(t, createTicketStub{patchStatus: http.StatusBadRequest})
	defer closeServer()

	ticket, schema := newTestTicket()

	// Before the fix this panicked: the update's nil return overwrote the
	// created item and was then dereferenced during ticket conversion, which
	// fails every ticket in the enclosing BulkCreateTickets batch.
	created, _, err := s.CreateTicket(context.Background(), ticket, schema)
	if err == nil {
		t.Fatal("expected the description update failure to be reported, got nil error")
	}
	if !strings.Contains(err.Error(), "failed to update catalog requested item description") {
		t.Errorf("error = %v, want it to mention the failed description update", err)
	}

	if created == nil {
		t.Fatal("created ticket is nil; the already-created ServiceNow request item was lost")
	}
	if created.GetId() != testRequestedItemID {
		t.Errorf("ticket id = %q, want %q", created.GetId(), testRequestedItemID)
	}
	if created.GetDisplayName() != testRequestedItemNumber {
		t.Errorf("ticket display name = %q, want %q", created.GetDisplayName(), testRequestedItemNumber)
	}
}

// The happy path still has to adopt the PATCH response, otherwise the returned
// ticket reports a stale description.
func TestCreateTicket_UsesUpdatedItemWhenDescriptionUpdateSucceeds(t *testing.T) {
	s, closeServer := newCreateTicketTestClient(t, createTicketStub{})
	defer closeServer()

	ticket, schema := newTestTicket()

	created, _, err := s.CreateTicket(context.Background(), ticket, schema)
	if err != nil {
		t.Fatalf("unexpected error creating ticket: %v", err)
	}
	if created.GetId() != testRequestedItemID {
		t.Errorf("ticket id = %q, want %q", created.GetId(), testRequestedItemID)
	}
	if created.GetDescription() != "description after update" {
		t.Errorf("ticket description = %q, want the updated description", created.GetDescription())
	}
}

// A failed label read is the other way the request item's id used to go missing:
// the conversion returns a usable ticket alongside its error, and CreateTicket
// discarded it. C1 then has no id for a request item that exists downstream, so
// its retry orders a second one.
func TestCreateTicket_KeepsCreatedItemWhenLabelFetchFails(t *testing.T) {
	s, closeServer := newCreateTicketTestClient(t, createTicketStub{labelStatus: http.StatusForbidden})
	defer closeServer()

	ticket, schema := newTestTicket()

	created, _, err := s.CreateTicket(context.Background(), ticket, schema)
	if err == nil {
		t.Fatal("expected the label fetch failure to be reported, got nil error")
	}
	if !strings.Contains(err.Error(), "failed to get labels for requested item") {
		t.Errorf("error = %v, want it to mention the failed label fetch", err)
	}

	if created == nil {
		t.Fatal("created ticket is nil; the already-created ServiceNow request item was lost")
	}
	if created.GetId() != testRequestedItemID {
		t.Errorf("ticket id = %q, want %q", created.GetId(), testRequestedItemID)
	}
}

// Every best-effort failure has to survive into the returned error. Reporting
// only the last one hides why a ticket came back incomplete.
func TestCreateTicket_ReportsEveryBestEffortFailure(t *testing.T) {
	s, closeServer := newCreateTicketTestClient(t, createTicketStub{
		patchStatus: http.StatusBadRequest,
		labelStatus: http.StatusForbidden,
	})
	defer closeServer()

	ticket, schema := newTestTicket()

	created, _, err := s.CreateTicket(context.Background(), ticket, schema)
	if err == nil {
		t.Fatal("expected both failures to be reported, got nil error")
	}
	for _, want := range []string{
		"failed to update catalog requested item description",
		"failed to get labels for requested item",
	} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %v, want it to mention %q", err, want)
		}
	}

	if created == nil {
		t.Fatal("created ticket is nil; the already-created ServiceNow request item was lost")
	}
	if created.GetId() != testRequestedItemID {
		t.Errorf("ticket id = %q, want %q", created.GetId(), testRequestedItemID)
	}
}

func TestServiceCatalogRequestItemToTicket_NilItemReturnsError(t *testing.T) {
	s := &ServiceNow{}

	if _, _, err := s.serviceCatalogRequestItemToTicket(context.Background(), nil); err == nil {
		t.Fatal("expected an error for a nil requested item, got nil")
	}
}
