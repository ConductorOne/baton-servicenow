package servicenow

import (
	"errors"
	"testing"
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
