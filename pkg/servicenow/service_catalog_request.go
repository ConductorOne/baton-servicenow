package servicenow

import (
	"context"
	"errors"
	"fmt"

	"github.com/conductorone/baton-sdk/pkg/annotations"
)

var ErrLabelNotFound = errors.New("label not found")

// Note on multi-call methods below: annotations carry at most one
// RateLimitDescription (annotations.WithRateLimiting uses Update, which
// replaces by type), and annotations.Merge appends rather than replaces -- two
// merged bags would leave Pick returning the stale entry. So methods that issue
// several requests return the annotations from the *last* one, which reflects
// the most recent state of the limit.

type FieldOption func(catalogItemRequestPayload *OrderItemPayload)

func WithCustomField(id string, value interface{}) FieldOption {
	return func(catalogItemRequestPayload *OrderItemPayload) {
		if catalogItemRequestPayload.Variables == nil {
			catalogItemRequestPayload.Variables = make(map[string]interface{})
		}
		catalogItemRequestPayload.Variables[id] = value
	}
}

func (c *Client) GetServiceCatalogRequest(ctx context.Context, requestId string) (*ServiceCatalogRequest, annotations.Annotations, error) {
	var serviceCatalogRequestResponse ServiceCatalogRequestResponse
	_, annos, err := c.get(
		ctx,
		c.apiURL(ServiceCatalogRequestDetailsBaseUrl, c.deployment, requestId),
		&serviceCatalogRequestResponse,
		WithIncludeExternalRefLink(),
	)
	if err != nil {
		return nil, annos, err
	}
	return &serviceCatalogRequestResponse.Result, annos, nil
}

func (c *Client) GetServiceCatalogRequestItem(ctx context.Context, requestItemId string) (*RequestedItem, annotations.Annotations, error) {
	var requestItemResponse RequestItemResponse
	_, annos, err := c.get(
		ctx,
		c.apiURL(ServiceCatalogRequestedItemDetailsBaseUrl, c.deployment, requestItemId),
		&requestItemResponse,
		WithIncludeExternalRefLink(),
	)
	if err != nil {
		return nil, annos, err
	}
	return &requestItemResponse.Result, annos, nil
}

func (c *Client) GetServiceCatalogRequestedItemForRequest(ctx context.Context, serviceCatalogRequestId string) (*RequestedItem, annotations.Annotations, error) {
	requestItemsResponse, _, annos, err := c.GetServiceCatalogRequestItems(ctx,
		WithPageLimit(1),
		WithQuery(fmt.Sprintf("request=%s", serviceCatalogRequestId)),
		WithIncludeExternalRefLink(),
	)
	if err != nil {
		return nil, annos, err
	}
	if len(requestItemsResponse) == 0 {
		return nil, annos, errors.New("no request item found for request")
	}
	return &requestItemsResponse[0], annos, nil
}

func (c *Client) UpdateServiceCatalogRequestItem(ctx context.Context, requestItemId string, payload *RequestedItemUpdatePayload) (*RequestedItem, annotations.Annotations, error) {
	var requestItemResponse RequestItemResponse
	annos, err := c.patch(
		ctx,
		c.apiURL(ServiceCatalogRequestedItemDetailsBaseUrl, c.deployment, requestItemId),
		&requestItemResponse,
		&payload,
		WithIncludeResponseBody(),
		WithIncludeExternalRefLink(),
	)
	if err != nil {
		return nil, annos, err
	}
	return &requestItemResponse.Result, annos, nil
}

func (c *Client) GetServiceCatalogRequestItems(ctx context.Context, reqOptions ...ReqOpt) ([]RequestedItem, string, annotations.Annotations, error) {
	var requestItemsResponse RequestItemsResponse
	nextPageToken, annos, err := c.get(
		ctx,
		c.apiURL(ServiceCatalogRequestedItemBaseUrl, c.deployment),
		&requestItemsResponse,
		reqOptions...,
	)
	if err != nil {
		return nil, "", annos, err
	}
	return requestItemsResponse.Result, nextPageToken, annos, nil
}

func (c *Client) GetCatalogItems(ctx context.Context, paginationVars *PaginationVars) ([]CatalogItem, string, annotations.Annotations, error) {
	var catalogItemsResponse CatalogItemsResponse
	reqOpts := []ReqOpt{
		WithPageLimit(paginationVars.Limit),
		WithOffset(paginationVars.Offset),
	}
	for k, v := range c.TicketSchemaFilters {
		reqOpts = append(reqOpts, WithQueryParam(k, v))
	}
	nextPageToken, annos, err := c.get(
		ctx,
		c.apiURL(ServiceCatalogItemBaseUrl, c.deployment),
		&catalogItemsResponse,
		reqOpts...,
	)
	if err != nil {
		return nil, "", annos, err
	}
	return catalogItemsResponse.Result, nextPageToken, annos, nil
}

func (c *Client) GetCatalogItem(ctx context.Context, catalogItemId string) (*CatalogItem, annotations.Annotations, error) {
	var catalogItemResponse CatalogItemResponse
	_, annos, err := c.get(
		ctx,
		c.apiURL(ServiceCatalogItemGetUrl, c.deployment, catalogItemId),
		&catalogItemResponse,
	)
	if err != nil {
		return nil, annos, err
	}
	return &catalogItemResponse.Result, annos, nil
}

func (c *Client) GetCatalogItemVariables(ctx context.Context, catalogItemId string) ([]CatalogItemVariable, annotations.Annotations, error) {
	var catalogItemVariablesResponse CatalogItemVariablesResponse
	_, annos, err := c.get(
		ctx,
		c.apiURL(ServiceCatalogItemVariablesUrl, c.deployment, catalogItemId),
		&catalogItemVariablesResponse,
	)
	if err != nil {
		return nil, annos, err
	}
	return catalogItemVariablesResponse.Result, annos, nil
}

// Creating a service catalog request requires:
// 1. Add catalog item to cart (with all required variables).
// 2. Submit cart order.
func (c *Client) CreateServiceCatalogRequest(ctx context.Context, catalogItemId string, payload *OrderItemPayload) (*RequestedItem, annotations.Annotations, error) {
	requestInfo, annos, err := c.OrderItemNow(ctx, catalogItemId, payload)
	if err != nil {
		return nil, annos, err
	}
	requestItem, annos, err := c.GetServiceCatalogRequestedItemForRequest(ctx, requestInfo.RequestID)
	if err != nil {
		return nil, annos, err
	}
	return requestItem, annos, nil
}

func (c *Client) OrderItemNow(ctx context.Context, catalogItemId string, payload *OrderItemPayload) (*RequestInfo, annotations.Annotations, error) {
	var orderCatalogItemResponse OrderCatalogItemResponse
	annos, err := c.post(
		ctx,
		c.apiURL(ServiceCatalogOrderItemUrl, c.deployment, catalogItemId),
		&orderCatalogItemResponse,
		&payload,
		WithIncludeResponseBody(),
	)
	if err != nil {
		return nil, annos, err
	}
	return &orderCatalogItemResponse.Result, annos, nil
}

func (c *Client) AddLabelsToRequest(ctx context.Context, requestedItemId string, labels []string) (annotations.Annotations, error) {
	var annos annotations.Annotations
	for _, label := range labels {
		_, labelAnnos, err := c.AddLabelToRequest(ctx, requestedItemId, label)
		annos = labelAnnos
		if err != nil {
			return annos, err
		}
	}
	return annos, nil
}

func (c *Client) AddLabelToRequest(ctx context.Context, requestedItemId string, label string) (*BaseResource, annotations.Annotations, error) {
	labelResp, annos, err := c.CreateLabel(ctx, label)
	if err != nil {
		return nil, annos, err
	}
	return c.addLabelToRequestedItem(ctx, requestedItemId, labelResp.Id)
}

func (c *Client) addLabelToRequestedItem(ctx context.Context, requestedItemId string, labelId string) (*BaseResource, annotations.Annotations, error) {
	var labelEntryResponse IDResponse
	annos, err := c.post(
		ctx,
		c.apiURL(LabelEntryBaseUrl, c.deployment),
		&labelEntryResponse,
		&LabelEntryPayload{
			Table:    "sc_req_item",
			TableKey: requestedItemId,
			Label:    labelId,
		},
		WithIncludeResponseBody(),
	)
	if err != nil {
		return nil, annos, fmt.Errorf("error adding label %s to requested item %s: %w", labelId, requestedItemId, err)
	}
	return &labelEntryResponse.Result, annos, nil
}

// Create label will return an error if it already exists.
// First fetch the label to check if it already exists.
func (c *Client) CreateLabel(ctx context.Context, label string) (*Label, annotations.Annotations, error) {
	labelResp, annos, err := c.GetLabel(ctx, label)
	if err == nil {
		return labelResp, annos, nil
	}
	if errors.Is(err, ErrLabelNotFound) {
		return c.createLabel(ctx, label)
	}
	return nil, annos, err
}

func (c *Client) GetLabel(ctx context.Context, label string) (*Label, annotations.Annotations, error) {
	var labelsResponse LabelsResponse
	_, annos, err := c.get(
		ctx,
		c.apiURL(LabelBaseUrl, c.deployment),
		&labelsResponse,
		WithQuery(fmt.Sprintf("name=%s", label)),
	)
	if err != nil {
		return nil, annos, fmt.Errorf("error fetching label '%s': %w", label, err)
	}
	if len(labelsResponse.Result) == 0 {
		return nil, annos, ErrLabelNotFound
	}
	return &labelsResponse.Result[0], annos, nil
}

func (c *Client) createLabel(ctx context.Context, label string) (*Label, annotations.Annotations, error) {
	var labelResponse LabelResponse
	annos, err := c.post(
		ctx,
		c.apiURL(LabelBaseUrl, c.deployment),
		&labelResponse,
		&Label{
			ViewableBy: "everyone",
			Name:       label,
		},
		WithIncludeResponseBody(),
	)
	if err != nil {
		return nil, annos, fmt.Errorf("error creating label '%s': %w", label, err)
	}
	return &labelResponse.Result, annos, nil
}

func (c *Client) GetLabelsForRequestedItem(ctx context.Context, requestedItemId string) ([]string, annotations.Annotations, error) {
	var labelResponse LabelEntriesLabelNameResponse
	_, annos, err := c.get(
		ctx,
		c.apiURL(LabelEntryBaseUrl, c.deployment),
		&labelResponse,
		WithQuery(fmt.Sprintf("table=sc_req_item^table_key=%s", requestedItemId)),
		WithFields("label.name"),
	)
	if err != nil {
		return nil, annos, err
	}
	labelStrings := make([]string, 0, len(labelResponse.Result))
	for _, label := range labelResponse.Result {
		labelStrings = append(labelStrings, label.LabelName)
	}
	return labelStrings, annos, nil
}

func (c *Client) GetServiceCatalogRequestedItemStates(ctx context.Context) ([]RequestItemState, annotations.Annotations, error) {
	var catalogsResponse RequestedItemStateResponse
	_, annos, err := c.get(
		ctx,
		c.apiURL(ChoiceBaseUrl, c.deployment),
		&catalogsResponse,
		WithQuery("name=task^element=state^language=en^inactive=false"),
		WithFields("label,value"),
	)
	if err != nil {
		return nil, annos, err
	}
	return catalogsResponse.Result, annos, nil
}

// Unused.
func (c *Client) GetCatalogs(ctx context.Context, paginationVars PaginationVars) ([]Catalog, string, annotations.Annotations, error) {
	var catalogsResponse CatalogsResponse
	nextPageToken, annos, err := c.get(
		ctx,
		c.apiURL(ServiceCatalogListCatalogsUrl, c.deployment),
		&catalogsResponse,
		WithPageLimit(paginationVars.Limit),
		WithOffset(paginationVars.Offset),
	)
	if err != nil {
		return nil, "", annos, err
	}
	return catalogsResponse.Result, nextPageToken, annos, nil
}
