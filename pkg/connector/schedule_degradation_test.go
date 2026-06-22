package connector

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-servicenow/pkg/servicenow"
)

// scheduleResourceFor builds a minimal schedule resource whose native id is the
// roster sys_id, as scheduleResource() does during sync.
func scheduleResourceFor(rosterID string) *v2.Resource {
	return &v2.Resource{Id: &v2.ResourceId{ResourceType: resourceTypeSchedule.Id, Resource: rosterID}}
}

// grantSlugs returns the trailing entitlement slug (member/on-call/manager) of
// each grant, for asserting which grant kinds survived degradation.
func grantSlugs(grants []*v2.Grant) []string {
	out := make([]string, 0, len(grants))
	for _, g := range grants {
		id := g.GetEntitlement().GetId()
		switch {
		case strings.HasSuffix(id, ":"+scheduleOnCall):
			out = append(out, scheduleOnCall)
		case strings.HasSuffix(id, ":"+scheduleManager):
			out = append(out, scheduleManager)
		case strings.HasSuffix(id, ":"+scheduleMember):
			out = append(out, scheduleMember)
		default:
			out = append(out, id)
		}
	}
	return out
}

func countSlug(slugs []string, want string) int {
	n := 0
	for _, s := range slugs {
		if s == want {
			n++
		}
	}
	return n
}

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

// newClient is a small helper for the degradation tests below.
func newClient(t *testing.T, srv *httptest.Server) *servicenow.Client {
	t.Helper()
	c, err := servicenow.NewClient(srv.Client(), "user:pass", "dev", nil, nil, nil, srv.URL)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	return c
}

// TestScheduleGrants_OnCallModuleAbsent_DegradesGracefully verifies that when
// the plugin is absent (cmn_rota_member returns "Invalid table"), Grants emits
// no grants and NO error — the rest of the sync is unaffected.
func TestScheduleGrants_OnCallModuleAbsent_DegradesGracefully(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.Contains(r.URL.Path, "cmn_rota_member") {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"message":"Invalid table cmn_rota_member"},"status":"failure"}`))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"result":[]}`))
	}))
	defer srv.Close()

	grants, _, _, err := scheduleBuilder(newClient(t, srv)).
		Grants(context.Background(), scheduleResourceFor("roster1"), &pagination.Token{})
	if err != nil {
		t.Fatalf("Grants must NOT error when the on-call module is absent, got: %v", err)
	}
	if len(grants) != 0 {
		t.Fatalf("expected 0 grants when module absent, got %d", len(grants))
	}
}

// TestScheduleGrants_PartialInstall_WhoIsOnCallFails simulates a partially
// reachable plugin: roster membership (cmn_rota_member) is readable, but the
// whoisoncall REST API is not (e.g. ACL/permission split). The member grants
// that DID resolve must survive, the on-call grant is skipped, and the sync is
// not aborted.
func TestScheduleGrants_PartialInstall_WhoIsOnCallFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "whoisoncall"):
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":{"message":"User Not Authorized"},"status":"failure"}`))
		case strings.Contains(r.URL.Path, "cmn_rota_member"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"result":[{"sys_id":"m1","member":"user1","roster":"roster1","order":"1"}]}`))
		case strings.Contains(r.URL.Path, "cmn_rota_roster"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"result":{"sys_id":"roster1","rota":"rota1"}}`))
		case strings.Contains(r.URL.Path, "sys_user_group"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"result":{"sys_id":"group1","manager":"mgr1"}}`))
		case strings.Contains(r.URL.Path, "cmn_rota"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"result":{"sys_id":"rota1","group":"group1"}}`))
		default:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"result":[]}`))
		}
	}))
	defer srv.Close()

	grants, _, _, err := scheduleBuilder(newClient(t, srv)).
		Grants(context.Background(), scheduleResourceFor("roster1"), &pagination.Token{})
	if err != nil {
		t.Fatalf("Grants must degrade (not error) when whoisoncall is unreachable, got: %v", err)
	}
	slugs := grantSlugs(grants)
	if countSlug(slugs, scheduleMember) != 1 {
		t.Fatalf("expected the resolved member grant to survive, got slugs=%v", slugs)
	}
	if countSlug(slugs, scheduleOnCall) != 0 {
		t.Fatalf("expected NO on-call grant when whoisoncall failed, got slugs=%v", slugs)
	}
	if countSlug(slugs, scheduleManager) != 1 {
		t.Fatalf("expected the manager grant to still resolve, got slugs=%v", slugs)
	}
}

// TestScheduleGrants_PartialInstall_ManagerLookupFails simulates the manager
// resolution chain (roster->rota->group) being blocked while membership and
// whoisoncall succeed. Member and on-call grants survive; manager is skipped;
// the sync is not aborted.
func TestScheduleGrants_PartialInstall_ManagerLookupFails(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.Contains(r.URL.Path, "whoisoncall"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"result":[{"userId":"user1","order":1}]}`))
		case strings.Contains(r.URL.Path, "cmn_rota_member"):
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"result":[{"sys_id":"m1","member":"user1","roster":"roster1","order":"1"}]}`))
		case strings.Contains(r.URL.Path, "cmn_rota_roster"):
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":{"message":"User Not Authorized"},"status":"failure"}`))
		default:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"result":[]}`))
		}
	}))
	defer srv.Close()

	grants, _, _, err := scheduleBuilder(newClient(t, srv)).
		Grants(context.Background(), scheduleResourceFor("roster1"), &pagination.Token{})
	if err != nil {
		t.Fatalf("Grants must degrade (not error) when manager lookup is blocked, got: %v", err)
	}
	slugs := grantSlugs(grants)
	if countSlug(slugs, scheduleMember) != 1 {
		t.Fatalf("expected the member grant to survive, got slugs=%v", slugs)
	}
	if countSlug(slugs, scheduleOnCall) != 1 {
		t.Fatalf("expected the on-call grant to survive, got slugs=%v", slugs)
	}
	if countSlug(slugs, scheduleManager) != 0 {
		t.Fatalf("expected NO manager grant when the lookup failed, got slugs=%v", slugs)
	}
}
