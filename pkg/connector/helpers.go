package connector

import (
	"fmt"

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

func annotationsForUserResourceType() annotations.Annotations {
	annos := annotations.Annotations{}
	annos.Update(&v2.SkipEntitlementsAndGrants{})
	return annos
}

// annotationsForScheduleResourceType marks the schedule resource type opt-in.
func annotationsForScheduleResourceType() annotations.Annotations {
	annos := annotations.Annotations{}
	annos.Update(&v2.OptInRequired{})
	return annos
}

// parsePageToken returns the bag plus the seek position, decoded by the same codec
// that produced it (servicenow.ParseKeysetToken) and carrying ResourcesPageSize as
// the limit. A malformed token fails loudly rather than restarting: a wrong guess
// means silently wrong pagination.
func parsePageToken(i string, resourceID *v2.ResourceId) (*pagination.Bag, servicenow.KeysetPaginationVars, error) {
	b := &pagination.Bag{}
	if err := b.Unmarshal(i); err != nil {
		return nil, servicenow.KeysetPaginationVars{}, err
	}

	if b.Current() == nil {
		b.Push(pagination.PageState{
			ResourceTypeID: resourceID.ResourceType,
			ResourceID:     resourceID.Resource,
		})
	}

	lastID, offset, err := servicenow.ParseKeysetToken(b.PageToken())
	if err != nil {
		return nil, servicenow.KeysetPaginationVars{}, fmt.Errorf("baton-servicenow: %w", err)
	}

	return b, servicenow.KeysetPaginationVars{
		Limit:  ResourcesPageSize,
		LastID: lastID,
		Offset: offset,
	}, nil
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

func mapRotaMembers(resources []servicenow.RotaMember) []string {
	members := make([]string, len(resources))

	for i, r := range resources {
		members[i] = r.Member
	}

	return members
}
