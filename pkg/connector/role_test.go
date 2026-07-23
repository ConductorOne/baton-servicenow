package connector

import (
	"testing"

	"github.com/conductorone/baton-servicenow/pkg/servicenow"
)

// TestRoleResource_ProfileSurfacesOnResource guards against a regression
// from moving the role profile from a RoleTrait option to a resource-level
// option: the resource itself must still carry the profile fields.
func TestRoleResource_ProfileSurfacesOnResource(t *testing.T) {
	role := &servicenow.Role{
		BaseResource: servicenow.BaseResource{Id: "role-1"},
		Name:         "admin",
	}

	resource, err := roleResource(role)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	profile := resource.GetProfile().AsMap()
	if got := profile["role_name"]; got != "admin" {
		t.Errorf("profile[role_name] = %v, want %q", got, "admin")
	}
	if got := profile["role_id"]; got != "role-1" {
		t.Errorf("profile[role_id] = %v, want %q", got, "role-1")
	}
}
