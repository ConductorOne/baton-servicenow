package connector

import (
	"fmt"
	"strconv"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	"github.com/conductorone/baton-servicenow/pkg/servicenow"
)

var ResourcesPageSize = 50

func annotationsForUserResourceType() annotations.Annotations {
	annos := annotations.Annotations{}
	annos.Update(&v2.SkipEntitlementsAndGrants{})
	return annos
}

func handleNextPage(bag *pagination.Bag, page int) (string, error) {
	nextPage := fmt.Sprintf("%d", page)
	pageToken, err := bag.NextToken(nextPage)
	if err != nil {
		return "", err
	}

	return pageToken, nil
}

func parsePageToken(i string, resourceID *v2.ResourceId) (*pagination.Bag, int, error) {
	b := &pagination.Bag{}
	err := b.Unmarshal(i)
	if err != nil {
		return nil, 0, err
	}

	if b.Current() == nil {
		b.Push(pagination.PageState{
			ResourceTypeID: resourceID.ResourceType,
			ResourceID:     resourceID.Resource,
		})
	}

	page, err := convertPageToken(b.PageToken())
	if err != nil {
		return nil, 0, err
	}

	return b, page, nil
}

func handleRoleGrantsPagination(
	grants []*v2.Grant,
	bag *pagination.Bag,
) ([]*v2.Grant, string, annotations.Annotations, error) {
	bag.Pop()

	if bag.Current() == nil {
		return grants, "", nil, nil
	}

	nextPage, err := bag.Marshal()
	if err != nil {
		return nil, "", nil, err
	}

	return grants, nextPage, nil, nil
}

// convertPageToken converts a string token into an int.
func convertPageToken(token string) (int, error) {
	if token == "" {
		return 0, nil
	}

	return strconv.Atoi(token)
}

func mapUsers(resources []servicenow.UserToRole) []string {
	users := make([]string, len(resources))

	for i, r := range resources {
		users[i] = r.User
	}

	return users
}

func mapGroups(resources []servicenow.GroupToRole) []string {
	groups := make([]string, len(resources))

	for i, r := range resources {
		groups[i] = r.Group
	}

	return groups
}

func mapGroupMembers(resources []servicenow.GroupMember) []string {
	members := make([]string, len(resources))

	for i, r := range resources {
		members[i] = r.User
	}

	return members
}
