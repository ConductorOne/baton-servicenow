package servicenow

import (
	"strconv"
	"strings"
	"text/template"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"google.golang.org/protobuf/types/known/anypb"
)

// MergeAnnotations appends src to dst so annotations from several calls are all
// kept. RateLimitDescription is a single-value annotation, so it is not
// appended: dst keeps one, the most restrictive seen (an overlimit status wins,
// otherwise the lowest remaining quota), so a throttled call is never hidden by
// a later healthy one. An empty description (what the SDK parser yields for a
// response without rate-limit headers) never displaces a populated one.
func MergeAnnotations(dst *annotations.Annotations, src annotations.Annotations) {
	for _, a := range src {
		if !a.MessageIs(&v2.RateLimitDescription{}) {
			dst.Merge(a)
			continue
		}
		mergeRateLimit(dst, a)
	}
}

func mergeRateLimit(dst *annotations.Annotations, incoming *anypb.Any) {
	for i, existing := range *dst {
		if !existing.MessageIs(&v2.RateLimitDescription{}) {
			continue
		}
		var cur, next v2.RateLimitDescription
		if existing.UnmarshalTo(&cur) != nil || incoming.UnmarshalTo(&next) != nil {
			return
		}
		if moreRestrictive(&next, &cur) {
			(*dst)[i] = incoming
		}
		return
	}
	dst.Merge(incoming)
}

// isEmptyRateLimit reports whether d carries no information. The SDK parser
// returns STATUS_UNSPECIFIED with zero limit/remaining and a zero-time ResetAt
// for a response without rate-limit headers. A real zero quota is not empty: it
// carries a limit, a reset time or an overlimit status.
func isEmptyRateLimit(d *v2.RateLimitDescription) bool {
	return d.GetStatus() == v2.RateLimitDescription_STATUS_UNSPECIFIED &&
		d.GetLimit() == 0 &&
		d.GetRemaining() == 0 &&
		(!d.HasResetAt() || d.GetResetAt().AsTime().IsZero())
}

func moreRestrictive(next, cur *v2.RateLimitDescription) bool {
	if isEmptyRateLimit(next) {
		return false
	}
	if isEmptyRateLimit(cur) {
		return true
	}
	nextOver := next.GetStatus() == v2.RateLimitDescription_STATUS_OVERLIMIT
	curOver := cur.GetStatus() == v2.RateLimitDescription_STATUS_OVERLIMIT
	if nextOver != curOver {
		return nextOver
	}
	return next.GetRemaining() < cur.GetRemaining()
}

type URLParams map[string]string

func GenerateURL(baseURL string, params URLParams) (string, error) {
	tmpl, err := template.New("url").Parse(baseURL)
	if err != nil {
		return "", err
	}
	var urlBuilder strings.Builder
	err = tmpl.Execute(&urlBuilder, params)
	if err != nil {
		return "", err
	}
	return urlBuilder.String(), nil
}

func ConvertPageToken(token string) (int, error) {
	if token == "" {
		return 0, nil
	}
	return strconv.Atoi(token)
}
