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
// a later healthy one.
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

func moreRestrictive(next, cur *v2.RateLimitDescription) bool {
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
