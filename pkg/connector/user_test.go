package connector

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-servicenow/pkg/servicenow"
)

// TestUserResource_StatusSurfacesOnResource guards against a regression from
// switching the enabled/disabled status from a UserTrait option to a
// resource-level option: the resource itself, not just the trait, must still
// carry the correct enabled/disabled status for every Active value ServiceNow
// can send.
func TestUserResource_StatusSurfacesOnResource(t *testing.T) {
	tests := []struct {
		name   string
		active string
		want   v2.Status_ResourceStatus
	}{
		{"true", "true", v2.Status_RESOURCE_STATUS_ENABLED},
		{"True", "True", v2.Status_RESOURCE_STATUS_ENABLED},
		{"1", "1", v2.Status_RESOURCE_STATUS_ENABLED},
		{"false", "false", v2.Status_RESOURCE_STATUS_DISABLED},
		{"False", "False", v2.Status_RESOURCE_STATUS_DISABLED},
		{"0", "0", v2.Status_RESOURCE_STATUS_DISABLED},
		{"unknown value defaults to disabled", "unexpected", v2.Status_RESOURCE_STATUS_DISABLED},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			user := &servicenow.User{
				BaseResource: servicenow.BaseResource{Id: "user-1"},
				UserName:     "jdoe",
				Email:        "jdoe@example.com",
				Active:       tc.active,
			}

			resource, err := userResource(user)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			got := resource.GetStatus().GetStatus()
			if got != tc.want {
				t.Errorf("resource status = %v, want %v", got, tc.want)
			}
		})
	}
}
