package connector

import (
	"context"
	"errors"
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
	// TODO(lauren) should we use pt.size?
	offset, err := convertPageToken(pt.Token)
	if err != nil {
		return nil, "", nil, err
	}
	catalogItems, nextPageToken, err := s.client.GetCatalogItems(ctx,
		&servicenow.PaginationVars{Offset: offset},
	)
	if err != nil {
		return nil, "", nil, err
	}

	var ret []*v2.TicketSchema
	for _, catalogItem := range catalogItems {
		catalogItemSchema, err := s.schemaForCatalogItem(ctx, &catalogItem)
		if err != nil {
			return nil, "", nil, err
		}
		ret = append(ret, catalogItemSchema)
	}

	return ret, nextPageToken, nil, nil
}

func (s *ServiceNow) GetTicket(ctx context.Context, ticketId string) (*v2.Ticket, annotations.Annotations, error) {
	serviceCatalogRequestedItem, err := s.client.GetServiceCatalogRequestItem(ctx, ticketId)
	if err != nil {
		return nil, nil, err
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
		return nil, nil, errors.New("error: unable to create ticket, ticket is invalid")
	}

	createServiceCatalogRequestPayload := &servicenow.AddItemToCartPayload{Quantity: 1}

	// TODO(lauren) check values format
	// TODO(lauren) move to create client method
	for _, opt := range ticketOptions {
		opt(createServiceCatalogRequestPayload)
	}

	serviceCatalogRequest, err := s.client.CreateServiceCatalogRequest(ctx, catalogItemID, createServiceCatalogRequestPayload)
	if err != nil {
		return nil, nil, err
	}

	ticket, annos, err := s.serviceCatalogRequestItemToTicket(ctx, serviceCatalogRequest)
	if err != nil {
		return nil, nil, err
	}

	l.Info("created service catalog request", zap.Any("ticket", ticket))

	return ticket, annos, err
}

func (s *ServiceNow) GetTicketSchema(ctx context.Context, schemaID string) (*v2.TicketSchema, annotations.Annotations, error) {
	catalogItem, err := s.client.GetCatalogItem(ctx, schemaID)
	if err != nil {
		return nil, nil, err
	}
	schema, err := s.schemaForCatalogItem(ctx, catalogItem)
	if err != nil {
		return nil, nil, err
	}
	return schema, nil, nil
}

func (s *ServiceNow) schemaForCatalogItem(ctx context.Context, catalogItem *servicenow.CatalogItem) (*v2.TicketSchema, error) {
	l := ctxzap.Extract(ctx)

	var err error
	// TODO(lauren) we don't have type
	// TODO(lauren) make type a custom field
	var ticketTypes []*v2.TicketType

	customFields := make(map[string]*v2.TicketCustomField)

	customFields["catalog_item"] = sdkTicket.PickObjectValueFieldSchema(
		"catalog_item",
		"Catalog Item",
		true,
		[]*v2.TicketCustomFieldObjectValue{
			{
				Id:          catalogItem.SysID,
				DisplayName: catalogItem.Name,
			},
		},
	)

	// List catalog items doesn't include variables but get catalog item does
	// Get the catalog variables if not present
	variables := catalogItem.Variables
	if len(variables) == 0 {
		variables, err = s.client.GetCatalogItemVariables(ctx, catalogItem.SysID)
		if err != nil {
			return nil, err
		}
	}

	for _, v := range variables {
		cf, err := servicenow.ConvertVariableToSchemaCustomField(&v)
		if err != nil {
			l.Error("error converting variable to custom field for schema", zap.Any("v", v))
			return nil, err
		}
		// TODO(lauren) cf can be nil since we aren't handling all variable cases
		customFields[v.Name] = cf
	}

	ret := &v2.TicketSchema{
		Id:           catalogItem.SysID,
		DisplayName:  catalogItem.Name,
		Types:        ticketTypes,
		CustomFields: customFields,
	}

	return ret, nil
}

func (s *ServiceNow) serviceCatalogRequestToTicket(ctx context.Context, request *servicenow.ServiceCatalogRequest) (*v2.Ticket, annotations.Annotations, error) {
	// TODO(lauren) use OpenedAt instead?
	createdAt, err := time.Parse(time.DateTime, request.SysCreatedOn)
	if err != nil {
		return nil, nil, err
	}

	updatedAt, err := time.Parse(time.DateTime, request.SysUpdatedOn)
	if err != nil {
		return nil, nil, err
	}

	var completedAt *timestamppb.Timestamp
	if request.ClosedAt != "" {
		closedAt, err := time.Parse(time.DateTime, request.ClosedAt)
		if err != nil {
			return nil, nil, err
		}
		completedAt = timestamppb.New(closedAt)
	}

	// TODO(lauren) we have RequestedFor and OpenedBy
	// Do we want to set these on Assignees/Reporter?
	return &v2.Ticket{
		Id:          request.Id,
		DisplayName: request.Number, // catalog request does not have display name
		Description: request.Description,
		Assignees:   nil,
		Reporter:    nil,
		Status: &v2.TicketStatus{
			Id:          request.State,
			DisplayName: request.RequestState,
		},
		Type:         nil,
		Labels:       nil,
		Url:          "",
		CustomFields: nil,
		CreatedAt:    timestamppb.New(createdAt),
		UpdatedAt:    timestamppb.New(updatedAt),
		CompletedAt:  completedAt,
	}, nil, nil
}

// TODO(lauren) if we want display name for status/other fields we can use sysparm_display_value=all query param
// or dot walking for specific fields (e.g sysparm_fields=cat_item.name)" \
func (s *ServiceNow) serviceCatalogRequestItemToTicket(ctx context.Context, requestedItem *servicenow.RequestedItem) (*v2.Ticket, annotations.Annotations, error) {
	// TODO(lauren) use OpenedAt instead?
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

	// TODO(lauren) we have RequestedFor and OpenedBy
	// Do we want to set these on Assignees/Reporter?
	return &v2.Ticket{
		Id:          requestedItem.Id,
		DisplayName: requestedItem.Number, // catalog request does not have display name
		Description: requestedItem.Description,
		Assignees:   nil,
		Reporter:    nil,
		Status: &v2.TicketStatus{
			Id: requestedItem.State,
		},
		Type:         nil,
		Labels:       []string{requestedItem.SysTags},
		Url:          "",
		CustomFields: nil,
		CreatedAt:    timestamppb.New(createdAt),
		UpdatedAt:    timestamppb.New(updatedAt),
		CompletedAt:  completedAt,
	}, nil, nil
}