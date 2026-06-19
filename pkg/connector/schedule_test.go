package connector

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
)

func TestEntitlementSlug(t *testing.T) {
	tests := []struct {
		name string
		ent  *v2.Entitlement
		want string
	}{
		{"member", &v2.Entitlement{Id: "schedule:c8dff571:member"}, "member"},
		{"on-call", &v2.Entitlement{Id: "schedule:c8dff571:on-call"}, "on-call"},
		{"owner", &v2.Entitlement{Id: "schedule:c8dff571:owner"}, "owner"},
		{"falls back to Slug when id has no colon", &v2.Entitlement{Id: "member", Slug: "member"}, "member"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := entitlementSlug(tt.ent); got != tt.want {
				t.Errorf("entitlementSlug(%q) = %q, want %q", tt.ent.Id, got, tt.want)
			}
		})
	}
}

func TestOnCallActionDate(t *testing.T) {
	got := onCallActionDate()
	if len(got) != 10 || got[4] != '-' || got[7] != '-' {
		t.Errorf("onCallActionDate() = %q, want YYYY-MM-DD", got)
	}
}
