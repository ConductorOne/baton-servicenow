package connector

import (
	"context"
	"fmt"
	"strings"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-servicenow/pkg/servicenow"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
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

// mapGroupMembers returns the member user IDs and how many rows were skipped for an empty user reference.
func mapGroupMembers(resources []servicenow.GroupMember) ([]string, int) {
	members := make([]string, 0, len(resources))
	skipped := 0

	for _, r := range resources {
		if isEmptyRef(r.User) {
			skipped++
			continue
		}
		members = append(members, r.User)
	}

	return members, skipped
}

// isEmptyRef reports a blank reference field, which ServiceNow returns for
// orphaned membership rows or values hidden by field-level read ACLs.
func isEmptyRef(ref string) bool {
	return strings.TrimSpace(ref) == ""
}

func logSkippedMemberships(ctx context.Context, table string, resource *v2.Resource, skipped int) {
	if skipped == 0 {
		return
	}

	ctxzap.Extract(ctx).Warn(
		"baton-servicenow: skipping membership rows with empty principal reference",
		zap.String("table", table),
		zap.String("resource_type", resource.Id.ResourceType),
		zap.String("resource_id", resource.Id.Resource),
		zap.Int("skipped_count", skipped),
	)
}
