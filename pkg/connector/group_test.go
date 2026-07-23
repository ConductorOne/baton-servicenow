package connector

import (
	"testing"

	"github.com/conductorone/baton-servicenow/pkg/servicenow"
)

// TestGroupResource_ProfileSurfacesOnResource guards against a regression
// from moving the group profile from a GroupTrait option to a resource-level
// option: the resource itself must still carry the profile fields.
func TestGroupResource_ProfileSurfacesOnResource(t *testing.T) {
	group := &servicenow.Group{
		BaseResource: servicenow.BaseResource{Id: "group-1"},
		Name:         "Admins",
		Description:  "Administrators group",
	}

	resource, err := groupResource(group)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	profile := resource.GetProfile().AsMap()
	if got := profile["group_name"]; got != "Admins" {
		t.Errorf("profile[group_name] = %v, want %q", got, "Admins")
	}
	if got := profile["group_id"]; got != "group-1" {
		t.Errorf("profile[group_id] = %v, want %q", got, "group-1")
	}
	if got := profile["group_description"]; got != "Administrators group" {
		t.Errorf("profile[group_description] = %v, want %q", got, "Administrators group")
	}
}
