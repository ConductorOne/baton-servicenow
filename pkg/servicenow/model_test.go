package servicenow

import (
	"encoding/json"
	"testing"
)

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
			name:  "with custom string fields",
			input: `{"sys_id":"abc123","user_name":"jdoe","email":"jdoe@example.com","active":"true","u_type":"consultant","u_department":"engineering"}`,
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
