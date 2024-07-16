package servicenow

import (
	"context"
	"errors"
	"fmt"
)

var LabelNotFoundErr = errors.New("label not found")

type FieldOption func(catalogItemRequestPayload *OrderItemPayload)

func WithCustomField(id string, value interface{}) FieldOption {
	return func(catalogItemRequestPayload *OrderItemPayload) {
		if catalogItemRequestPayload.Variables == nil {
			catalogItemRequestPayload.Variables = make(map[string]interface{})
		}
		catalogItemRequestPayload.Variables[id] = value
	}
}

func (c *Client) GetServiceCatalogRequest(ctx context.Context, requestId string) (*ServiceCatalogRequest, error) {
	var serviceCatalogRequestResponse ServiceCatalogRequestResponse
	_, err := c.get(
		ctx,
		fmt.Sprintf(ServiceCatalogRequestDetailsBaseUrl, c.deployment, requestId),
		&serviceCatalogRequestResponse,
		WithIncludeExternalRefLink(),
	)
	if err != nil {
		return nil, err
	}
	return &serviceCatalogRequestResponse.Result, nil
}

func (c *Client) GetServiceCatalogRequestItem(ctx context.Context, requestItemId string) (*RequestedItem, error) {
	var requestItemResponse RequestItemResponse
	_, err := c.get(
		ctx,
		fmt.Sprintf(ServiceCatalogRequestedItemDetailsBaseUrl, c.deployment, requestItemId),
		&requestItemResponse,
		WithIncludeExternalRefLink(),
	)
	if err != nil {
		return nil, err
	}
	return &requestItemResponse.Result, nil
}

func (c *Client) GetServiceCatalogRequestedItemForRequest(ctx context.Context, serviceCatalogRequestId string) (*RequestedItem, error) {
	requestItemsResponse, _, err := c.GetServiceCatalogRequestItems(ctx,
		WithPageLimit(1),
		WithQuery(fmt.Sprintf("request=%s", serviceCatalogRequestId)),
		WithIncludeExternalRefLink(),
	)
	if err != nil {
		return nil, err
	}
	if len(requestItemsResponse) == 0 {
		return nil, errors.New("no request item found for request")
	}
	return &requestItemsResponse[0], nil
}

func (c *Client) GetServiceCatalogRequestItems(ctx context.Context, reqOptions ...ReqOpt) ([]RequestedItem, string, error) {
	var requestItemsResponse RequestItemsResponse
	nextPageToken, err := c.get(
		ctx,
		fmt.Sprintf(ServiceCatalogRequestedItemBaseUrl, c.deployment),
		&requestItemsResponse,
		reqOptions...,
	)
	if err != nil {
		return nil, "", err
	}
	return requestItemsResponse.Result, nextPageToken, nil
}

func (c *Client) GetCatalogItems(ctx context.Context, paginationVars *PaginationVars) ([]CatalogItem, string, error) {
	var catalogItemsResponse CatalogItemsResponse
	nextPageToken, err := c.get(
		ctx,
		fmt.Sprintf(ServiceCatalogItemBaseUrl, c.deployment),
		&catalogItemsResponse,
		WithPageLimit(paginationVars.Limit),
		WithOffset(paginationVars.Offset),
	)
	if err != nil {
		return nil, "", err
	}
	return catalogItemsResponse.Result, nextPageToken, nil
}

func (c *Client) GetCatalogItem(ctx context.Context, catalogItemId string) (*CatalogItem, error) {
	var catalogItemResponse CatalogItemResponse
	_, err := c.get(
		ctx,
		fmt.Sprintf(ServiceCatalogItemGetUrl, c.deployment, catalogItemId),
		&catalogItemResponse,
	)
	if err != nil {
		return nil, err
	}
	return &catalogItemResponse.Result, nil
}

func (c *Client) GetCatalogItemVariables(ctx context.Context, catalogItemId string) ([]CatalogItemVariable, error) {
	var catalogItemVariablesResponse CatalogItemVariablesResponse
	_, err := c.get(
		ctx,
		fmt.Sprintf(ServiceCatalogItemVariablesUrl, c.deployment, catalogItemId),
		&catalogItemVariablesResponse,
	)
	if err != nil {
		return nil, err
	}
	return catalogItemVariablesResponse.Result, nil
}

// Creating a service catalog request requires:
// 1. Add catalog item to cart (with all required variables).
// 2. Submit cart order.
func (c *Client) CreateServiceCatalogRequest(ctx context.Context, catalogItemId string, payload *OrderItemPayload) (*RequestedItem, error) {
	requestInfo, err := c.OrderItemNow(ctx, catalogItemId, payload)
	if err != nil {
		return nil, err
	}
	requestItem, err := c.GetServiceCatalogRequestedItemForRequest(ctx, requestInfo.RequestID)
	if err != nil {
		return nil, err
	}
	return requestItem, nil
}

func (c *Client) OrderItemNow(ctx context.Context, catalogItemId string, payload *OrderItemPayload) (*RequestInfo, error) {
	var orderCatalogItemResponse OrderCatalogItemResponse
	err := c.post(
		ctx,
		fmt.Sprintf(ServiceCatalogOrderItemUrl, c.deployment, catalogItemId),
		&orderCatalogItemResponse,
		&payload,
		WithIncludeResponseBody(),
	)
	if err != nil {
		return nil, err
	}
	return &orderCatalogItemResponse.Result, nil
}

func (c *Client) AddLabelsToRequest(ctx context.Context, requestedItemId string, labels []string) error {
	for _, label := range labels {
		_, err := c.AddLabelToRequest(ctx, requestedItemId, label)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Client) AddLabelToRequest(ctx context.Context, requestedItemId string, label string) (*BaseResource, error) {
	labelResp, err := c.CreateLabel(ctx, label)
	if err != nil {
		return nil, err
	}
	return c.addLabelToRequestedItem(ctx, requestedItemId, labelResp.Id)
}

func (c *Client) addLabelToRequestedItem(ctx context.Context, requestedItemId string, labelId string) (*BaseResource, error) {
	var labelEntryResponse IDResponse
	err := c.post(
		ctx,
		fmt.Sprintf(LabelEntryBaseUrl, c.deployment),
		&labelEntryResponse,
		&LabelEntryPayload{
			Table:    "sc_req_item",
			TableKey: requestedItemId,
			Label:    labelId,
		},
		WithIncludeResponseBody(),
	)
	if err != nil {
		return nil, err
	}
	return &labelEntryResponse.Result, nil
}

// Create label will return an error if it already exists.
// First fetch the label to check if it already exists.
func (c *Client) CreateLabel(ctx context.Context, label string) (*Label, error) {
	labelResp, err := c.GetLabel(ctx, label)
	if err == nil {
		return labelResp, nil
	}
	if errors.Is(err, LabelNotFoundErr) {
		return c.createLabel(ctx, label)
	}
	return nil, err
}

func (c *Client) GetLabel(ctx context.Context, label string) (*Label, error) {
	var labelsResponse LabelsResponse
	_, err := c.get(
		ctx,
		fmt.Sprintf(LabelBaseUrl, c.deployment),
		&labelsResponse,
		WithQuery(fmt.Sprintf("name=%s", label)),
	)
	if err != nil {
		return nil, err
	}
	if len(labelsResponse.Result) == 0 {
		return nil, LabelNotFoundErr
	}
	return &labelsResponse.Result[0], nil
}

func (c *Client) createLabel(ctx context.Context, label string) (*Label, error) {
	var labelResponse LabelResponse
	err := c.post(
		ctx,
		fmt.Sprintf(LabelBaseUrl, c.deployment),
		&labelResponse,
		&Label{
			ViewableBy: "everyone",
			Name:       label,
		},
	)
	if err != nil {
		return nil, err
	}
	return &labelResponse.Result, nil
}

func (c *Client) GetLabelsForRequestedItem(ctx context.Context, requestedItemId string) ([]string, error) {
	var labelResponse LabelEntriesLabelNameResponse
	_, err := c.get(
		ctx,
		fmt.Sprintf(LabelEntryBaseUrl, c.deployment),
		&labelResponse,
		WithQuery(fmt.Sprintf("table=sc_req_item^table_key=%s", requestedItemId)),
		WithFields("label.name"),
	)
	if err != nil {
		return nil, err
	}
	labelStrings := make([]string, 0, len(labelResponse.Result))
	for _, label := range labelResponse.Result {
		labelStrings = append(labelStrings, label.LabelName)
	}
	return labelStrings, nil
}

func (c *Client) GetServiceCatalogRequestedItemStates(ctx context.Context) ([]RequestItemState, error) {
	var catalogsResponse RequestedItemStateResponse
	_, err := c.get(
		ctx,
		fmt.Sprintf(ChoiceBaseUrl, c.deployment),
		&catalogsResponse,
		WithQuery("name=task^element=state^language=en^inactive=false"),
		WithFields("label,value"),
	)
	if err != nil {
		return nil, err
	}
	return catalogsResponse.Result, nil
}

// Unused.
func (c *Client) GetCatalogs(ctx context.Context, paginationVars PaginationVars) ([]Catalog, string, error) {
	var catalogsResponse CatalogsResponse
	nextPageToken, err := c.get(
		ctx,
		fmt.Sprintf(ServiceCatalogListCatalogsUrl, c.deployment),
		&catalogsResponse,
		WithPageLimit(paginationVars.Limit),
		WithOffset(paginationVars.Offset),
	)
	if err != nil {
		return nil, "", err
	}
	return catalogsResponse.Result, nextPageToken, nil
}
