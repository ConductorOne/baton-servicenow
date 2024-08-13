package connector

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"time"

	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/pagination"
	sdkTicket "github.com/conductorone/baton-sdk/pkg/types/ticket"
	"github.com/conductorone/baton-servicenow/pkg/servicenow"
	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (s *ServiceNow) ListTicketSchemas(ctx context.Context, pt *pagination.Token) ([]*v2.TicketSchema, string, annotations.Annotations, error) {
	offset, err := convertPageToken(pt.Token)
	if err != nil {
		return nil, "", nil, err
	}
	catalogItems, nextPageToken, err := s.client.GetCatalogItems(ctx,
		&servicenow.PaginationVars{
			Limit:  pt.Size,
			Offset: offset,
		},
	)
	if err != nil {
		return nil, "", nil, fmt.Errorf("servicenow-connector: failed to get catalog items: %w", err)
	}

	requestedItemStates, err := s.client.GetServiceCatalogRequestedItemStates(ctx)
	if err != nil {
		return nil, "", nil, fmt.Errorf("servicenow-connector: failed to get catalog requested item states: %w", err)
	}

	ticketStatuses := requestedItemStatesToTicketStatus(requestedItemStates)

	var ret []*v2.TicketSchema
	for _, catalogItem := range catalogItems {
		catalogItem := catalogItem
		catalogItemSchema, err := s.schemaForCatalogItem(ctx, &catalogItem)
		if err != nil {
			return nil, "", nil, err
		}
		catalogItemSchema.Statuses = ticketStatuses
		ret = append(ret, catalogItemSchema)
	}

	return ret, nextPageToken, nil, nil
}

func (s *ServiceNow) GetTicket(ctx context.Context, ticketId string) (*v2.Ticket, annotations.Annotations, error) {
	serviceCatalogRequestedItem, err := s.client.GetServiceCatalogRequestItem(ctx, ticketId)
	if err != nil {
		return nil, nil, fmt.Errorf("servicenow-connector: failed to get catalog requested item %s: %w", ticketId, err)
	}
	ticket, annos, err := s.serviceCatalogRequestItemToTicket(ctx, serviceCatalogRequestedItem)
	if err != nil {
		return nil, nil, err
	}
	return ticket, annos, err
}

func (s *ServiceNow) CreateTicket(ctx context.Context, ticket *v2.Ticket, schema *v2.TicketSchema) (*v2.Ticket, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	ticketOptions := []servicenow.FieldOption{}

	ticketFields := ticket.GetCustomFields()

	var catalogItemID string

	for id, cf := range schema.GetCustomFields() {
		switch id {
		case "catalog_item":
			catalogItem, err := sdkTicket.GetPickObjectValue(ticketFields[id])
			if err != nil {
				return nil, nil, err
			}
			if catalogItem.GetId() == "" {
				return nil, nil, errors.New("error: unable to create ticket, catalog item is required")
			}
			catalogItemID = catalogItem.GetId()
		default:
			ticketField := ticketFields[id]

			// We need to handle this type differently so we only get the string value we set for "id"
			// The servicenow variable "choice" seem to only be strings (single select, multiselect)
			pick := ticketField.GetPickObjectValue()
			if pick != nil {
				val := pick.GetValue().GetId()
				ticketOptions = append(ticketOptions, servicenow.WithCustomField(cf.GetId(), val))
				continue
			}
			// TODO(lauren) handle multi pick differently also

			val, err := sdkTicket.GetCustomFieldValue(ticketFields[id])
			if err != nil {
				return nil, nil, err
			}
			// The ticket doesn't have this key set, so we skip it
			if val == nil {
				continue
			}
			ticketOptions = append(ticketOptions, servicenow.WithCustomField(cf.GetId(), val))
		}
	}

	valid, err := sdkTicket.ValidateTicket(ctx, schema, ticket)
	if err != nil {
		l.Error("error validating ticket", zap.Any("err", err), zap.Any("schema", schema), zap.Any("ticket", ticket))
		return nil, nil, err
	}
	if !valid {
		return nil, nil, errors.Join(errors.New("error: unable to create ticket, ticket is invalid"), sdkTicket.ErrTicketValidationError)
	}

	createServiceCatalogRequestPayload := &servicenow.OrderItemPayload{Quantity: 1, RequestedFor: ticket.GetRequestedFor().GetId().GetResource()}
	for _, opt := range ticketOptions {
		opt(createServiceCatalogRequestPayload)
	}

	serviceCatalogRequestedItem, err := s.client.CreateServiceCatalogRequest(ctx, catalogItemID, createServiceCatalogRequestPayload)
	if err != nil {
		return nil, nil, fmt.Errorf("servicenow-connector: failed to create service catalog request %s: %w", catalogItemID, err)
	}
	err = s.client.AddLabelsToRequest(ctx, serviceCatalogRequestedItem.Id, ticket.Labels)
	if err != nil {
		return nil, nil, fmt.Errorf("servicenow-connector: failed to add labels to request for catalog requested item %s: %w", serviceCatalogRequestedItem.Id, err)
	}

	serviceCatalogRequestedItem, err = s.client.UpdateServiceCatalogRequestItem(ctx,
		serviceCatalogRequestedItem.Id,
		&servicenow.RequestedItemUpdatePayload{
			Description: ticket.Description,
		},
	)
	if err != nil {
		return nil, nil, fmt.Errorf("servicenow-connector: failed to update catalog requested item %s: %w", serviceCatalogRequestedItem.Id, err)
	}

	ticket, annos, err := s.serviceCatalogRequestItemToTicket(ctx, serviceCatalogRequestedItem)
	if err != nil {
		return nil, nil, err
	}

	l.Info("created service catalog request", zap.Any("ticket", ticket))

	return ticket, annos, err
}

func (s *ServiceNow) GetTicketSchema(ctx context.Context, schemaID string) (*v2.TicketSchema, annotations.Annotations, error) {
	catalogItem, err := s.client.GetCatalogItem(ctx, schemaID)
	if err != nil {
		return nil, nil, fmt.Errorf("servicenow-connector: failed to get catalog item %s: %w", schemaID, err)
	}
	requestedItemStates, err := s.client.GetServiceCatalogRequestedItemStates(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("servicenow-connector: failed to get catalog requested item states: %w", err)
	}
	ticketStatuses := requestedItemStatesToTicketStatus(requestedItemStates)
	schema, err := s.schemaForCatalogItem(ctx, catalogItem)
	if err != nil {
		return nil, nil, err
	}
	schema.Statuses = ticketStatuses
	return schema, nil, nil
}

func (s *ServiceNow) schemaForCatalogItem(ctx context.Context, catalogItem *servicenow.CatalogItem) (*v2.TicketSchema, error) {
	// TODO(lauren) make type a custom field
	var ticketTypes []*v2.TicketType

	customFields := make(map[string]*v2.TicketCustomField)

	customFields["catalog_item"] = sdkTicket.PickObjectValueFieldSchema(
		"catalog_item",
		"Catalog Item",
		true,
		[]*v2.TicketCustomFieldObjectValue{
			{
				Id:          catalogItem.Id,
				DisplayName: catalogItem.Name,
			},
		},
	)

	// TODO(lauren) move this logic so we dont get for empty variables when listing schemas
	// List catalog items doesn't include variables but get catalog item does
	// Get the catalog variables if not present
	var err error
	variables := catalogItem.Variables
	if len(variables) == 0 {
		variables, err = s.client.GetCatalogItemVariables(ctx, catalogItem.Id)
		if err != nil {
			return nil, fmt.Errorf("servicenow-connector: failed to get catalog variables for catalog item %s: %w", catalogItem.Id, err)
		}
	}

	for _, v := range variables {
		v := v
		cf, err := servicenow.ConvertVariableToSchemaCustomField(ctx, &v)
		if err != nil {
			return nil, fmt.Errorf("servicenow-connector: failed to convert variable to custom field for catalog item %s: %w", catalogItem.Id, err)
		}
		// cf can be nil since we aren't handling all variable cases (if not required)
		if cf == nil {
			continue
		}
		customFields[v.Name] = cf
	}

	ret := &v2.TicketSchema{
		Id:           catalogItem.Id,
		DisplayName:  catalogItem.Name,
		Types:        ticketTypes,
		CustomFields: customFields,
	}

	return ret, nil
}

func (s *ServiceNow) serviceCatalogRequestItemToTicket(ctx context.Context, requestedItem *servicenow.RequestedItem) (*v2.Ticket, annotations.Annotations, error) {
	createdAt, err := time.Parse(time.DateTime, requestedItem.SysCreatedOn)
	if err != nil {
		return nil, nil, err
	}

	updatedAt, err := time.Parse(time.DateTime, requestedItem.SysUpdatedOn)
	if err != nil {
		return nil, nil, err
	}

	var completedAt *timestamppb.Timestamp
	if requestedItem.ClosedAt != "" {
		closedAt, err := time.Parse(time.DateTime, requestedItem.ClosedAt)
		if err != nil {
			return nil, nil, err
		}
		completedAt = timestamppb.New(closedAt)
	}

	labels, err := s.client.GetLabelsForRequestedItem(ctx, requestedItem.Id)
	if err != nil {
		return nil, nil, fmt.Errorf("servicenow-connector: failed to get labels for requested item %s: %w", requestedItem.Id, err)
	}

	// TODO(lauren) if we want to set approvers for assignees must query sysapproval_approver table
	// TODO(lauren) dont need to get user if we dot walk

	return &v2.Ticket{
		Id:          requestedItem.Id,
		DisplayName: requestedItem.Number, // catalog request does not have display name
		Description: requestedItem.Description,
		Assignees:   nil,
		Reporter:    nil,
		Status: &v2.TicketStatus{
			Id: requestedItem.State,
		},
		Labels:       labels,
		Url:          s.generateRequestedItemURL(requestedItem),
		CustomFields: nil,
		CreatedAt:    timestamppb.New(createdAt),
		UpdatedAt:    timestamppb.New(updatedAt),
		CompletedAt:  completedAt,
	}, nil, nil
}

func (s *ServiceNow) generateRequestedItemURL(requestedItem *servicenow.RequestedItem) string {
	params := url.Values{"sys_id": []string{requestedItem.Id}}
	requestUrl := url.URL{
		Scheme:   "https",
		Host:     s.client.GetBaseURL(),
		Path:     "sc_req_item.do",
		RawQuery: params.Encode(),
	}
	return requestUrl.String()
}

func requestedItemStatesToTicketStatus(states []servicenow.RequestItemState) []*v2.TicketStatus {
	ticketStatuses := make([]*v2.TicketStatus, 0, len(states))
	for _, state := range states {
		ticketStatuses = append(ticketStatuses, &v2.TicketStatus{
			Id:          state.Value,
			DisplayName: state.Label,
		})
	}
	return ticketStatuses
}
