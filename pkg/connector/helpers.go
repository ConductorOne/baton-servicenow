package connector

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"unicode"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-servicenow/pkg/servicenow"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

// logSkippedMemberships samples per table: an orphaned row is not actionable by the
// customer and can recur on every page of every role and group.
func logSkippedMemberships(ctx context.Context, sampler *perKeySampledDebug, table string, resource *v2.Resource, skipped int) {
	if skipped == 0 {
		return
	}

	sampler.log(ctx, table,
		"baton-servicenow: skipping membership rows with empty principal reference",
		zap.String("table", table),
		zap.String("resource_type", resource.Id.ResourceType),
		zap.String("resource_id", resource.Id.Resource),
		zap.Int("skipped_count", skipped),
	)
}

// normalizeTicketSchemaIDs trims each configured schema ID, drops blanks and
// removes duplicates, keeping the first occurrence so configured order holds.
func normalizeTicketSchemaIDs(ids []string) []string {
	seen := make(map[string]struct{}, len(ids))
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		id = strings.TrimSpace(id)
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, id)
	}
	return out
}

// MaxTicketSchemaIDs is the most distinct ticket-schema-ids entries accepted,
// counted after trimming and removing blanks and duplicates. It keeps a
// selection within the 100 schemas the SDK currently hands to C1, so the
// connector never silently drops configured IDs.
const MaxTicketSchemaIDs = 100

// maxLoggedIDLength bounds how much of a configured ID is echoed into a log line.
const maxLoggedIDLength = 64

// ticketSchemaIDPattern matches the shape of a ServiceNow sys_id. It is kept to
// plain alphanumerics rather than 32 hex characters because non-canonical
// sys_ids exist, but it excludes everything that could change the request path
// ('/', '?', '#', '%', '.', whitespace).
var ticketSchemaIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// validateTicketSchemaIDCount rejects a normalized selection larger than
// MaxTicketSchemaIDs. It never truncates.
func validateTicketSchemaIDCount(ids []string) error {
	if len(ids) <= MaxTicketSchemaIDs {
		return nil
	}
	return status.Errorf(codes.InvalidArgument,
		"baton-servicenow: ticket-schema-ids lists %d distinct IDs (after trimming blanks and removing duplicates) but at most %d are supported; remove at least %d",
		len(ids), MaxTicketSchemaIDs, len(ids)-MaxTicketSchemaIDs)
}

// splitTicketSchemaIDs separates IDs that are safe to place in a catalog item
// URL path segment from those that are not, keeping the configured order.
func splitTicketSchemaIDs(ids []string) ([]string, []string) {
	valid := make([]string, 0, len(ids))
	var malformed []string
	for _, id := range ids {
		if ticketSchemaIDPattern.MatchString(id) {
			valid = append(valid, id)
		} else {
			malformed = append(malformed, id)
		}
	}
	return valid, malformed
}

// loggableID makes a configured value safe to echo into a log line: bounded
// length and no control characters. zap escapes the rest.
func loggableID(id string) string {
	var b strings.Builder
	n := 0
	for _, r := range id {
		if n >= maxLoggedIDLength {
			b.WriteString("...")
			break
		}
		if unicode.IsControl(r) {
			r = '?'
		}
		b.WriteRune(r)
		n++
	}
	return b.String()
}

// vendorItemUnavailableMessage is the body ServiceNow returns, with HTTP 400,
// for a catalog item that does not exist, is malformed, is inactive or is not
// accessible to the caller (observed live; see tickets/CXH-1166 evidence).
const vendorItemUnavailableMessage = "catalog item is not valid or security constraints prevent access"

// itemSkipReason reports whether err from a request scoped to one configured
// catalog item means that item alone is unusable, and if so why. Only
// confirmed item-specific responses qualify: 404, 403, and the known vendor 400
// body. Anything else (401, 429, timeouts, 5xx, transport errors, other 400s)
// is operational and must be returned to the caller.
func itemSkipReason(err error) (string, bool) {
	code := status.Code(err)
	if code == codes.NotFound {
		return "not found (HTTP 404)", true
	}
	if code == codes.PermissionDenied {
		return "access denied (HTTP 403); the integration user may lack access to this item", true
	}
	if code == codes.InvalidArgument && isVendorItemUnavailable(err) {
		return "ServiceNow reported the item is not valid or not accessible (HTTP 400: it may not exist, be inactive, or be restricted)", true
	}
	return "", false
}

// isVendorItemUnavailable matches the item-specific vendor 400 body.
func isVendorItemUnavailable(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), vendorItemUnavailableMessage)
}
