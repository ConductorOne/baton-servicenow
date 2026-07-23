package connector

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
)

// marshalPageToken builds the serialized bag token an SDK-driven sync would
// pass back into List()/Grants() on the next page, given a resourceID and
// whatever page token was checkpointed for it.
func marshalPageToken(t *testing.T, resourceID *v2.ResourceId, checkpointedToken string) string {
	t.Helper()

	b := &pagination.Bag{}
	if err := b.Unmarshal(""); err != nil {
		t.Fatalf("unexpected error unmarshaling empty token: %v", err)
	}
	b.Push(pagination.PageState{
		ResourceTypeID: resourceID.ResourceType,
		ResourceID:     resourceID.Resource,
	})

	marshaled, err := b.NextToken(checkpointedToken)
	if err != nil {
		t.Fatalf("unexpected error building checkpointed token: %v", err)
	}
	return marshaled
}

// TestParsePageToken_TokenValidation covers the two shapes a checkpointed
// page token can take: (1) a real sys_id passes through as the seek
// cursor, normalized to lowercase; (2) anything else -- including a
// pre-keyset numeric offset token -- fails loudly instead of guessing,
// since a wrong guess here means silently wrong pagination results, not
// just a restart.
func TestParsePageToken_TokenValidation(t *testing.T) {
	resourceID := &v2.ResourceId{ResourceType: "role"}

	t.Run("legacy numeric offset token fails loudly", func(t *testing.T) {
		legacyToken := marshalPageToken(t, resourceID, "150")

		_, _, err := parsePageToken(legacyToken, resourceID)
		if err == nil {
			t.Fatalf("expected an error for a pre-keyset offset token, got nil")
		}
	})

	t.Run("token with injected query condition fails loudly instead of restarting", func(t *testing.T) {
		maliciousToken := marshalPageToken(t, resourceID, "abc^grantable=false")

		_, _, err := parsePageToken(maliciousToken, resourceID)
		if err == nil {
			t.Fatalf("expected an error for a malformed token, got nil (silently restarting risks an infinite loop if this recurs)")
		}
	})

	t.Run("real sys_id cursor passes through unchanged", func(t *testing.T) {
		sysID := "cc6f85b5ebc31300a210a2505206fec0"
		keysetToken := marshalPageToken(t, resourceID, sysID)

		_, lastID, err := parsePageToken(keysetToken, resourceID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if lastID != sysID {
			t.Errorf("lastID = %q, want %q", lastID, sysID)
		}
	})

	t.Run("uppercase sys_id cursor is normalized to lowercase", func(t *testing.T) {
		sysID := "CC6F85B5EBC31300A210A2505206FEC0"
		keysetToken := marshalPageToken(t, resourceID, sysID)

		_, lastID, err := parsePageToken(keysetToken, resourceID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := "cc6f85b5ebc31300a210a2505206fec0"
		if lastID != want {
			t.Errorf("lastID = %q, want %q (ServiceNow's collation is case-insensitive; normalize for a stable cursor)", lastID, want)
		}
	})

	t.Run("empty token (first page) passes through unchanged", func(t *testing.T) {
		_, lastID, err := parsePageToken("", resourceID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if lastID != "" {
			t.Errorf("lastID = %q, want empty", lastID)
		}
	})
}
