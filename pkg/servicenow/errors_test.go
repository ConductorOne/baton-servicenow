package servicenow

import (
	"errors"
	"fmt"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsInvalidTableError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{
			"invalid table (real client error shape)",
			errors.New(`baton-servicenow: request failed with status 400: {"error":{"message":"Invalid table cmn_rota_roster","detail":null},"status":"failure"}`),
			true,
		},
		{"other 400", errors.New("baton-servicenow: request failed with status 400: bad request"), false},
		{"server error", errors.New("baton-servicenow: request failed with status 500: boom"), false},
	}
	for _, tc := range cases {
		if got := IsInvalidTableError(tc.err); got != tc.want {
			t.Errorf("%s: IsInvalidTableError = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestIsAccessDeniedError(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{
			"permission denied (direct status error)",
			status.Error(codes.PermissionDenied, "request failed with status 403"),
			true,
		},
		{
			// The real shape: doHTTPRequest wraps the uhttp grpc status error
			// (403 -> codes.PermissionDenied) with %w, so IsAccessDeniedError
			// must see through the wrapper via status.Code's errors.As unwrap.
			"permission denied wrapped (real client error shape)",
			fmt.Errorf("baton-servicenow: request failed with status 403: %s: %w",
				`{"error":{"message":"Operation against file 'cmn_rota_roster' was aborted by ACL"}}`,
				status.Error(codes.PermissionDenied, "Forbidden")),
			true,
		},
		{"not found", status.Error(codes.NotFound, "missing"), false},
		// A 403 in the message but not carried as a grpc code must NOT match:
		// it's the code, not the string, that means access denied.
		{"plain 403 string, no grpc code", errors.New("baton-servicenow: request failed with status 403: nope"), false},
	}
	for _, tc := range cases {
		if got := IsAccessDeniedError(tc.err); got != tc.want {
			t.Errorf("%s: IsAccessDeniedError = %v, want %v", tc.name, got, tc.want)
		}
	}
}
