package servicenow

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/uhttp"
)

const testCatalogItemID = "catalog-item-1"

func newServiceCatalogRequestClient(t *testing.T, handler http.Handler) *Client {
	t.Helper()

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	client, err := NewClient(uhttp.NewBaseHttpClient(server.Client()), "Basic dGVzdDp0ZXN0", "dev0", nil, nil, nil, server.URL)
	if err != nil {
		t.Fatalf("unexpected error creating client: %v", err)
	}
	return client
}

func writeServiceCatalogResponse(t *testing.T, w http.ResponseWriter, response any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		t.Errorf("failed to encode response: %v", err)
	}
}

func TestCreateServiceCatalogRequest_OneStepCheckout(t *testing.T) {
	var cartOrSubmitCalled bool
	client := newServiceCatalogRequestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/sn_sc/servicecatalog/cart":
			cartOrSubmitCalled = true
			w.WriteHeader(http.StatusInternalServerError)
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/order_now"):
			writeServiceCatalogResponse(t, w, OrderCatalogItemResponse{Result: RequestInfo{RequestID: "request-1"}})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/cart/submit_order"):
			cartOrSubmitCalled = true
			w.WriteHeader(http.StatusInternalServerError)
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/now/table/sc_req_item"):
			writeServiceCatalogResponse(t, w, RequestItemsResponse{Result: []RequestedItem{{BaseResource: BaseResource{Id: "ritm-1"}}}})
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))

	item, _, err := client.CreateServiceCatalogRequest(context.Background(), testCatalogItemID, &OrderItemPayload{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if item.Id != "ritm-1" {
		t.Errorf("requested item ID = %q, want %q", item.Id, "ritm-1")
	}
	if cartOrSubmitCalled {
		t.Fatal("one-step checkout must not read or submit the cart")
	}
}

func TestCreateServiceCatalogRequest_TwoStepCheckout(t *testing.T) {
	cartReads := 0
	client := newServiceCatalogRequestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/sn_sc/servicecatalog/cart":
			cartReads++
			// ServiceNow stores cart items beneath dynamic recurrence buckets.
			writeServiceCatalogResponse(t, w, map[string]any{"result": map[string]any{
				"cart_id": "cart-1",
				"monthly": map[string]any{"items": []map[string]string{{"catalog_item_id": testCatalogItemID}}},
			}})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/order_now"):
			writeServiceCatalogResponse(t, w, OrderCatalogItemResponse{Result: RequestInfo{CartID: "cart-1"}})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/cart/submit_order"):
			if r.ContentLength != 0 {
				t.Errorf("submit_order Content-Length = %d, want no request body", r.ContentLength)
			}
			writeServiceCatalogResponse(t, w, OrderCatalogItemResponse{Result: RequestInfo{RequestID: "request-2"}})
		case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/now/table/sc_req_item"):
			writeServiceCatalogResponse(t, w, RequestItemsResponse{Result: []RequestedItem{{BaseResource: BaseResource{Id: "ritm-2"}}}})
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))

	item, _, err := client.CreateServiceCatalogRequest(context.Background(), testCatalogItemID, &OrderItemPayload{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if item.Id != "ritm-2" {
		t.Errorf("requested item ID = %q, want %q", item.Id, "ritm-2")
	}
	if cartReads != 1 {
		t.Errorf("cart reads = %d, want 1", cartReads)
	}
}

func TestCreateServiceCatalogRequest_RejectsExistingCartItems(t *testing.T) {
	orderCalled := false
	client := newServiceCatalogRequestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/sn_sc/servicecatalog/cart":
			writeServiceCatalogResponse(t, w, map[string]any{"result": map[string]any{
				"cart_id": "leftover-cart",
				"none": map[string]any{"items": []map[string]string{
					{"catalog_item_id": "interrupted-item"},
					{"catalog_item_id": testCatalogItemID},
				}},
			}})
		case r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/order_now"):
			orderCalled = true
			writeServiceCatalogResponse(t, w, OrderCatalogItemResponse{Result: RequestInfo{CartID: "cart-1"}})
		default:
			t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))

	_, _, err := client.CreateServiceCatalogRequest(context.Background(), testCatalogItemID, &OrderItemPayload{})
	if err == nil || !strings.Contains(err.Error(), "clear the ServiceNow cart before retrying") {
		t.Fatalf("error = %v, want explicit cart-remediation error", err)
	}
	if !orderCalled {
		t.Fatal("order_now must stage the item before detecting the existing cart item")
	}
}

func TestCreateServiceCatalogRequest_RejectsMissingRequestID(t *testing.T) {
	client := newServiceCatalogRequestClient(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasSuffix(r.URL.Path, "/order_now") {
			writeServiceCatalogResponse(t, w, OrderCatalogItemResponse{})
			return
		}
		t.Errorf("unexpected request: %s %s", r.Method, r.URL.Path)
		w.WriteHeader(http.StatusNotFound)
	}))

	_, _, err := client.CreateServiceCatalogRequest(context.Background(), testCatalogItemID, &OrderItemPayload{})
	if err == nil || !strings.Contains(err.Error(), "did not include a request ID") {
		t.Fatalf("error = %v, want missing request ID error", err)
	}
}
