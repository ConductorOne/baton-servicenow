package servicenow

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestRotaMember_Unmarshal(t *testing.T) {
	const input = `{"sys_id":"mem123","roster":"ros456","member":"usr789","order":"1"}`

	var got RotaMember
	if err := json.Unmarshal([]byte(input), &got); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if got.Id != "mem123" {
		t.Errorf("Id = %q, want mem123", got.Id)
	}
	if got.Roster != "ros456" {
		t.Errorf("Roster = %q, want ros456", got.Roster)
	}
	if got.Member != "usr789" {
		t.Errorf("Member = %q, want usr789", got.Member)
	}
	if got.Order != "1" {
		t.Errorf("Order = %q, want 1", got.Order)
	}
}

func TestRoster_Unmarshal(t *testing.T) {
	const input = `{"sys_id":"ros456","name":"Primary","rota":"rot111"}`

	var got Roster
	if err := json.Unmarshal([]byte(input), &got); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Id != "ros456" || got.Name != "Primary" || got.Rota != "rot111" {
		t.Errorf("got %+v, want {Id:ros456 Name:Primary Rota:rot111}", got)
	}
}

func TestUser_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name               string
		input              string
		wantUserName       string
		wantEmail          string
		wantCustomFields   map[string]string
		wantCustomFieldLen int
	}{
		{
			name:               "standard fields only",
			input:              `{"sys_id":"abc123","user_name":"jdoe","email":"jdoe@example.com","first_name":"John","last_name":"Doe","roles":"admin","active":"true"}`,
			wantUserName:       "jdoe",
			wantEmail:          "jdoe@example.com",
			wantCustomFields:   map[string]string{},
			wantCustomFieldLen: 0,
		},
		{
			name:         "with custom string fields",
			input:        `{"sys_id":"abc123","user_name":"jdoe","email":"jdoe@example.com","active":"true","u_type":"consultant","u_department":"engineering"}`,
			wantUserName: "jdoe",
			wantEmail:    "jdoe@example.com",
			wantCustomFields: map[string]string{
				"u_type":       "consultant",
				"u_department": "engineering",
			},
			wantCustomFieldLen: 2,
		},
		{
			name:               "non-string u_ field is skipped",
			input:              `{"sys_id":"abc123","user_name":"jdoe","email":"jdoe@example.com","active":"true","u_count":42,"u_type":"contractor"}`,
			wantUserName:       "jdoe",
			wantEmail:          "jdoe@example.com",
			wantCustomFields:   map[string]string{"u_type": "contractor"},
			wantCustomFieldLen: 1,
		},
		{
			name:               "null u_ field is skipped",
			input:              `{"sys_id":"abc123","user_name":"jdoe","email":"jdoe@example.com","active":"true","u_type":null}`,
			wantUserName:       "jdoe",
			wantEmail:          "jdoe@example.com",
			wantCustomFields:   map[string]string{},
			wantCustomFieldLen: 0,
		},
		{
			name:               "object u_ field is skipped",
			input:              `{"sys_id":"abc123","user_name":"jdoe","email":"jdoe@example.com","active":"true","u_manager":{"link":"http://example.com","value":"mgr123"}}`,
			wantUserName:       "jdoe",
			wantEmail:          "jdoe@example.com",
			wantCustomFields:   map[string]string{},
			wantCustomFieldLen: 0,
		},
		{
			name:               "non-u_ extra fields are ignored",
			input:              `{"sys_id":"abc123","user_name":"jdoe","email":"jdoe@example.com","active":"true","custom_field":"value","x_type":"something"}`,
			wantUserName:       "jdoe",
			wantEmail:          "jdoe@example.com",
			wantCustomFields:   map[string]string{},
			wantCustomFieldLen: 0,
		},
		{
			name:               "empty string u_ field is captured",
			input:              `{"sys_id":"abc123","user_name":"jdoe","email":"jdoe@example.com","active":"true","u_type":""}`,
			wantUserName:       "jdoe",
			wantEmail:          "jdoe@example.com",
			wantCustomFields:   map[string]string{"u_type": ""},
			wantCustomFieldLen: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var user User
			err := json.Unmarshal([]byte(tc.input), &user)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if user.UserName != tc.wantUserName {
				t.Errorf("UserName = %q, want %q", user.UserName, tc.wantUserName)
			}
			if user.Email != tc.wantEmail {
				t.Errorf("Email = %q, want %q", user.Email, tc.wantEmail)
			}
			if len(user.CustomFields) != tc.wantCustomFieldLen {
				t.Errorf("len(CustomFields) = %d, want %d", len(user.CustomFields), tc.wantCustomFieldLen)
			}
			for k, want := range tc.wantCustomFields {
				got, ok := user.CustomFields[k]
				if !ok {
					t.Errorf("CustomFields missing key %q", k)
				} else if got != want {
					t.Errorf("CustomFields[%q] = %q, want %q", k, got, want)
				}
			}
		})
	}
}

func TestPrepareRotaMemberFilter(t *testing.T) {
	tests := []struct {
		name      string
		rosterId  string
		memberId  string
		wantQuery string
	}{
		{"roster only", "ros456", "", "roster=ros456"},
		{"both", "ros456", "usr789", "roster=ros456^member=usr789"},
		{"member only", "", "usr789", "member=usr789"},
		{"neither", "", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := prepareRotaMemberFilter(tt.rosterId, tt.memberId)
			if got.Query != tt.wantQuery {
				t.Errorf("Query = %q, want %q", got.Query, tt.wantQuery)
			}
		})
	}
}

func TestOnCallMember_Unmarshal(t *testing.T) {
	const input = `{"userId":"usr1","roster":"ros1","rota":"rot1","group":"grp1","order":1,"isOverride":false}`
	var got OnCallMember
	if err := json.Unmarshal([]byte(input), &got); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.UserId != "usr1" || got.Roster != "ros1" || got.Rota != "rot1" || got.Group != "grp1" {
		t.Errorf("refs = %+v, want userId=usr1 roster=ros1 rota=rot1 group=grp1", got)
	}
	if got.Order != 1 {
		t.Errorf("Order = %d, want 1", got.Order)
	}
}

func TestRota_Unmarshal(t *testing.T) {
	const input = `{"sys_id":"rot1","name":"Primary Rotation","group":"grp1"}`
	var got Rota
	if err := json.Unmarshal([]byte(input), &got); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Id != "rot1" || got.Name != "Primary Rotation" || got.Group != "grp1" {
		t.Errorf("got %+v, want {Id:rot1 Name:Primary Rotation Group:grp1}", got)
	}
}

func TestGroup_ManagerUnmarshal(t *testing.T) {
	const input = `{"sys_id":"grp1","name":"Eng","description":"d","manager":"mgr1"}`
	var got Group
	if err := json.Unmarshal([]byte(input), &got); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Manager != "mgr1" {
		t.Errorf("Manager = %q, want mgr1", got.Manager)
	}
}

func TestOnCallActionPayloads_Marshal(t *testing.T) {
	addBytes, _ := json.Marshal(OnCallAddMemberPayload{Member: "u1", Rosters: "r1", Rota: "rt1", FromDate: "2026-06-19"})
	add := string(addBytes)
	for _, want := range []string{`"member":"u1"`, `"rosters":"r1"`, `"rota":"rt1"`, `"from_date":"2026-06-19"`} {
		if !strings.Contains(add, want) {
			t.Errorf("add payload %s missing %s", add, want)
		}
	}
	rmBytes, _ := json.Marshal(OnCallRemoveMemberPayload{User: "u1", Rosters: "r1", FromDate: "2026-06-19", DeleteMember: "true"})
	rm := string(rmBytes)
	for _, want := range []string{`"user":"u1"`, `"rosters":"r1"`, `"delete_member":"true"`} {
		if !strings.Contains(rm, want) {
			t.Errorf("remove payload %s missing %s", rm, want)
		}
	}
	// rota is omitempty: absent when empty
	if strings.Contains(rm, `"rota"`) {
		t.Errorf("remove payload should omit empty rota: %s", rm)
	}
}
