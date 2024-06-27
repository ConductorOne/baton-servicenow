package servicenow

import (
	"context"
	"errors"
	"fmt"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
)

type FieldOption func(catalogItemRequestPayload *AddItemToCartPayload)

func WithCustomField(id string, value interface{}) FieldOption {
	return func(catalogItemRequestPayload *AddItemToCartPayload) {
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
		[]QueryParam{
			prepareServiceCatalogFilters(),
		}...,
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
		[]QueryParam{
			prepareServiceCatalogFilters(),
		}...,
	)
	if err != nil {
		return nil, err
	}
	return &requestItemResponse.Result, nil
}

func (c *Client) GetServiceCatalogRequestedItemForRequest(ctx context.Context, serviceCatalogRequestId string) (*RequestedItem, error) {
	requestItemsResponse, _, err := c.GetServiceCatalogRequestItems(ctx,
		PaginationVars{Limit: 1},
		prepareServiceCatalogFilters(WithQuery(fmt.Sprintf("request=%s", serviceCatalogRequestId))))
	if err != nil {
		return nil, err
	}
	if len(requestItemsResponse) == 0 {
		return nil, errors.New("no request item found for request")
	}
	return &requestItemsResponse[0], nil
}

func (c *Client) GetServiceCatalogRequestItems(ctx context.Context, paginationVars PaginationVars, filter *FilterVars) ([]RequestedItem, string, error) {
	var requestItemsResponse RequestItemsResponse
	nextPageToken, err := c.get(
		ctx,
		fmt.Sprintf(ServiceCatalogRequestedItemBaseUrl, c.deployment),
		&requestItemsResponse,
		[]QueryParam{
			&paginationVars,
			filter,
		}...,
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
		[]QueryParam{
			paginationVars,
			prepareServiceCatalogFilters(),
		}...,
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
		[]QueryParam{
			prepareServiceCatalogFilters(),
		}...,
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
		[]QueryParam{
			prepareServiceCatalogFilters(),
		}...,
	)
	if err != nil {
		return nil, err
	}
	return catalogItemVariablesResponse.Result, nil
}

// Creating a service catalog request requires:
// 1. Add catalog item to cart (with all required variables)
// 2. Submit cart order
func (c *Client) CreateServiceCatalogRequest(ctx context.Context, catalogItemId string, payload *AddItemToCartPayload) (*RequestedItem, error) {
	l := ctxzap.Extract(ctx)
	_, err := c.AddItemToCart(ctx, catalogItemId, payload)
	if err != nil {
		return nil, err
	}
	requestInfo, err := c.SubmitCartOrder(ctx)
	if err != nil {
		return nil, err
	}
	catalogRequest, err := c.GetServiceCatalogRequest(ctx, requestInfo.RequestID)
	if err != nil {
		return nil, err
	}

	l.Info("created catalog request", zap.Any("catalogRequest", catalogRequest))

	requestItem, err := c.GetServiceCatalogRequestedItemForRequest(ctx, requestInfo.RequestID)
	if err != nil {
		return nil, err
	}

	return requestItem, nil
}

func (c *Client) AddItemToCart(ctx context.Context, catalogItemId string, payload *AddItemToCartPayload) (*Cart, error) {
	var addItemToCartResponse AddItemToCartResponse
	err := c.post(
		ctx,
		fmt.Sprintf(ServiceCatalogAddItemToCartUrl, c.deployment, catalogItemId),
		&addItemToCartResponse,
		&payload,
	)
	if err != nil {
		return nil, err
	}
	return &addItemToCartResponse.Result, nil
}

func (c *Client) SubmitCartOrder(ctx context.Context) (*RequestInfo, error) {
	var submitCartOrderResponse SubmitCartOrderResponse
	err := c.post(
		ctx,
		fmt.Sprintf(ServiceCatalogCartSubmitOrder, c.deployment),
		&submitCartOrderResponse,
		nil,
	)
	if err != nil {
		return nil, err
	}
	return &submitCartOrderResponse.Result, nil
}

// Unused
func (c *Client) GetCatalogs(ctx context.Context, paginationVars PaginationVars) ([]Catalog, string, error) {
	var catalogsResponse CatalogsResponse
	nextPageToken, err := c.get(
		ctx,
		fmt.Sprintf(ServiceCatalogListCatalogsUrl, c.deployment),
		&catalogsResponse,
		[]QueryParam{
			&paginationVars,
			prepareServiceCatalogFilters(),
		}...,
	)
	if err != nil {
		return nil, "", err
	}
	return catalogsResponse.Result, nextPageToken, nil
}
