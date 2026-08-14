package connector

import (
	"testing"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-servicenow/pkg/servicenow"
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

// The shapes a checkpointed page token can take. Anything else must fail loudly:
// a wrong guess means silently wrong pagination, not just a restart.
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

		_, page, err := parsePageToken(keysetToken, resourceID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if page.LastID != sysID {
			t.Errorf("lastID = %q, want %q", page.LastID, sysID)
		}
	})

	t.Run("uppercase sys_id cursor is normalized to lowercase", func(t *testing.T) {
		sysID := "CC6F85B5EBC31300A210A2505206FEC0"
		keysetToken := marshalPageToken(t, resourceID, sysID)

		_, page, err := parsePageToken(keysetToken, resourceID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		want := "cc6f85b5ebc31300a210a2505206fec0"
		if page.LastID != want {
			t.Errorf("lastID = %q, want %q (ServiceNow's collation is case-insensitive; normalize for a stable cursor)", page.LastID, want)
		}
	})

	t.Run("empty token (first page) passes through unchanged", func(t *testing.T) {
		_, page, err := parsePageToken("", resourceID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if page.LastID != "" {
			t.Errorf("lastID = %q, want empty", page.LastID)
		}
	})

	t.Run("cursor with skip offset yields both halves", func(t *testing.T) {
		sysID := "cc6f85b5ebc31300a210a2505206fec0"
		// Built by the client layer's encoder, so a format change can't pass here.
		keysetToken := marshalPageToken(t, resourceID, servicenow.EncodeKeysetToken(sysID, 400))

		_, page, err := parsePageToken(keysetToken, resourceID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if page.LastID != sysID {
			t.Errorf("lastID = %q, want %q", page.LastID, sysID)
		}
		if page.Offset != 400 {
			t.Errorf("offset = %d, want 400", page.Offset)
		}
	})

	t.Run("skip offset with no cursor is accepted", func(t *testing.T) {
		// The first window of a listing can be the emptied one: offset, no cursor.
		keysetToken := marshalPageToken(t, resourceID, servicenow.EncodeKeysetToken("", 200))

		_, page, err := parsePageToken(keysetToken, resourceID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if page.LastID != "" {
			t.Errorf("lastID = %q, want empty", page.LastID)
		}
		if page.Offset != 200 {
			t.Errorf("offset = %d, want 200", page.Offset)
		}
	})

	t.Run("cursor with non-numeric offset fails loudly", func(t *testing.T) {
		keysetToken := marshalPageToken(t, resourceID, "cc6f85b5ebc31300a210a2505206fec0:abc")

		_, _, err := parsePageToken(keysetToken, resourceID)
		if err == nil {
			t.Fatalf("expected an error for a non-numeric offset, got nil")
		}
	})

	t.Run("plain cursor reports offset zero", func(t *testing.T) {
		keysetToken := marshalPageToken(t, resourceID, servicenow.EncodeKeysetToken("cc6f85b5ebc31300a210a2505206fec0", 0))

		_, page, err := parsePageToken(keysetToken, resourceID)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if page.Offset != 0 {
			t.Errorf("offset = %d, want 0 (a plain cursor is the fast path)", page.Offset)
		}
	})
}
