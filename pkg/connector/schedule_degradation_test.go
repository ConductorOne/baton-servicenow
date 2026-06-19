package connector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-servicenow/pkg/servicenow"
)

// TestScheduleList_OnCallModuleAbsent_DegradesGracefully verifies that when the
// On-Call Scheduling plugin is not installed — cmn_rota_roster returns
// ServiceNow's HTTP 400 "Invalid table" error — schedule List returns no
// resources and NO error, so the sync of users/groups/roles (separate resource
// types) is unaffected rather than aborted.
func TestScheduleList_OnCallModuleAbsent_DegradesGracefully(t *testing.T) {
	var rosterHits int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "cmn_rota_roster") {
			rosterHits++
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"message":"Invalid table cmn_rota_roster","detail":null},"status":"failure"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result":[]}`))
	}))
	defer srv.Close()

	client, err := servicenow.NewClient(srv.Client(), "user:pass", "dev", nil, nil, nil, srv.URL)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}

	resources, _, _, err := scheduleBuilder(client).List(context.Background(), nil, &pagination.Token{})
	if err != nil {
		t.Fatalf("List must NOT error when the on-call module is absent, got: %v", err)
	}
	if rosterHits == 0 {
		t.Fatal("test did not exercise the cmn_rota_roster path")
	}
	if len(resources) != 0 {
		t.Fatalf("expected 0 schedule resources when module absent, got %d", len(resources))
	}
}
