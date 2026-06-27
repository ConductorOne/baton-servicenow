package servicenow

import "testing"

func TestAppendUpdatedSince(t *testing.T) {
	tests := []struct {
		name  string
		query string
		ts    string
		want  string
	}{
		{
			name:  "empty ts is a no-op (full pull)",
			query: "grantable=true",
			ts:    "",
			want:  "grantable=true",
		},
		{
			name:  "empty query yields clause plus ascending order",
			query: "",
			ts:    "2026-01-02 03:04:05",
			want:  "sys_updated_on>=2026-01-02 03:04:05^ORDERBYsys_updated_on",
		},
		{
			name:  "ANDs with existing query via caret plus ascending order",
			query: "grantable=true",
			ts:    "2026-01-02 03:04:05",
			want:  "grantable=true^sys_updated_on>=2026-01-02 03:04:05^ORDERBYsys_updated_on",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := appendUpdatedSince(tt.query, tt.ts); got != tt.want {
				t.Fatalf("appendUpdatedSince(%q,%q) = %q, want %q", tt.query, tt.ts, got, tt.want)
			}
		})
	}
}
