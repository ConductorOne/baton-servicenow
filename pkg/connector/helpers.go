package connector

import (
	"fmt"
	"regexp"
	"strings"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-servicenow/pkg/servicenow"
)

// ResourcesPageSize is the page size for the 6 keyset-paginated Table API
// listings (users, roles, groups, membership). 200 cuts request count ~4x
// vs the old 50 without pushing all the way to the Table API's 10,000 max.
//
// Note: GetUsers, GetUserToGroup, and GetUserToRole don't actually get 200
// when AllowedDomains is configured -- cappedForDomainFilter forces their
// limit back down to domainFilteredPageSize (50) for enumeration calls,
// since the dot-walk filter (user.emailENDSWITH) can't use the sys_id
// index and a bigger page means more server-side scan per request.
const ResourcesPageSize = 200
const TicketSchemasPageSize = 25

// sysIDPattern matches a ServiceNow sys_id: 32 hex characters. ServiceNow's
// storage collation is case-insensitive, so either case is accepted and
// normalized to lowercase for a stable cursor.
var sysIDPattern = regexp.MustCompile(`^[0-9a-fA-F]{32}$`)

func annotationsForUserResourceType() annotations.Annotations {
	annos := annotations.Annotations{}
	annos.Update(&v2.SkipEntitlementsAndGrants{})
	return annos
}

// parsePageToken returns the bag along with its current page token, which for
// keyset-paginated resources (users, roles, groups, membership) is the last
// seen sys_id rather than a numeric offset.
func parsePageToken(i string, resourceID *v2.ResourceId) (*pagination.Bag, string, error) {
	b := &pagination.Bag{}
	err := b.Unmarshal(i)
	if err != nil {
		return nil, "", err
	}

	if b.Current() == nil {
		b.Push(pagination.PageState{
			ResourceTypeID: resourceID.ResourceType,
			ResourceID:     resourceID.Resource,
		})
	}

	token := b.PageToken()
	if token == "" {
		return b, "", nil
	}

	// A valid sys_id cursor normalizes to lowercase; anything else --
	// including a pre-keyset numeric offset token -- fails loudly rather
	// than guessing, since a wrong guess here means silently wrong
	// pagination results, not just a restart.
	if !sysIDPattern.MatchString(token) {
		return nil, "", fmt.Errorf("baton-servicenow: malformed page token %q", token)
	}
	return b, strings.ToLower(token), nil
}

// convertPageToken converts a string token into an int.
func convertPageToken(token string) (int, error) {
	return servicenow.ConvertPageToken(token)
}

func mapGroupMembers(resources []servicenow.GroupMember) []string {
	members := make([]string, len(resources))

	for i, r := range resources {
		members[i] = r.User
	}

	return members
}
