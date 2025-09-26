package connector

import (
	"context"
	"fmt"

	"github.com/grpc-ecosystem/go-grpc-middleware/logging/zap/ctxzap"
	"go.uber.org/zap"

	config "github.com/conductorone/baton-sdk/pb/c1/config/v1"
	v2 "github.com/conductorone/baton-sdk/pb/c1/connector/v2"
	"github.com/conductorone/baton-sdk/pkg/actions"
	"github.com/conductorone/baton-sdk/pkg/annotations"
	"github.com/conductorone/baton-sdk/pkg/connectorbuilder"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	ActionEnableUser  = "enable_user"
	ActionDisableUser = "disable_user"
)

var enableUserAction = &v2.BatonActionSchema{
	Name: ActionEnableUser,
	Arguments: []*config.Field{
		{
			Name:        "userId",
			DisplayName: "User ID",
			Field:       &config.Field_StringField{},
			IsRequired:  true,
		},
	},
	ReturnTypes: []*config.Field{
		{
			Name:        "success",
			DisplayName: "Success",
			Field:       &config.Field_BoolField{},
		},
	},
	ActionType: []v2.ActionType{
		v2.ActionType_ACTION_TYPE_ACCOUNT,
		v2.ActionType_ACTION_TYPE_ACCOUNT_ENABLE,
	},
}

var disableUserAction = &v2.BatonActionSchema{
	Name: ActionDisableUser,
	Arguments: []*config.Field{
		{
			Name:        "userId",
			DisplayName: "User ID",
			Field:       &config.Field_StringField{},
			IsRequired:  true,
		},
	},
	ReturnTypes: []*config.Field{
		{
			Name:        "success",
			DisplayName: "Success",
			Field:       &config.Field_BoolField{},
		},
	},
	ActionType: []v2.ActionType{
		v2.ActionType_ACTION_TYPE_ACCOUNT,
		v2.ActionType_ACTION_TYPE_ACCOUNT_DISABLE,
	},
}

func (s *ServiceNow) RegisterActionManager(ctx context.Context) (connectorbuilder.CustomActionManager, error) {
	actionManager := actions.NewActionManager(ctx)

	err := actionManager.RegisterAction(ctx, enableUserAction.Name, enableUserAction, s.enableUser)
	if err != nil {
		return nil, err
	}

	err = actionManager.RegisterAction(ctx, disableUserAction.Name, disableUserAction, s.disableUser)
	if err != nil {
		return nil, err
	}

	return actionManager, nil
}

func (s *ServiceNow) enableUser(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	if args == nil {
		return nil, nil, fmt.Errorf("arguments cannot be nil")
	}

	if args.Fields == nil {
		return nil, nil, fmt.Errorf("arguments fields cannot be nil")
	}

	userId, ok := args.Fields["userId"]
	if !ok {
		return nil, nil, fmt.Errorf("missing required argument userId")
	}

	if userId == nil {
		return nil, nil, fmt.Errorf("userId value cannot be nil")
	}

	userIdStr := userId.GetStringValue()
	if userIdStr == "" {
		return nil, nil, fmt.Errorf("userId cannot be empty")
	}

	l.Info("enabling user", zap.String("userId", userIdStr))

	err := s.client.UpdateUserActiveStatus(ctx, userIdStr, true)
	if err != nil {
		l.Error("failed to enable user", zap.String("userId", userIdStr), zap.Error(err))
		return nil, nil, fmt.Errorf("failed to enable user %s: %w", userIdStr, err)
	}

	response := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"success": structpb.NewBoolValue(true),
		},
	}
	return response, nil, nil
}

func (s *ServiceNow) disableUser(ctx context.Context, args *structpb.Struct) (*structpb.Struct, annotations.Annotations, error) {
	l := ctxzap.Extract(ctx)

	if args == nil {
		return nil, nil, fmt.Errorf("arguments cannot be nil")
	}

	if args.Fields == nil {
		return nil, nil, fmt.Errorf("arguments fields cannot be nil")
	}

	userId, ok := args.Fields["userId"]
	if !ok {
		return nil, nil, fmt.Errorf("missing required argument userId")
	}

	if userId == nil {
		return nil, nil, fmt.Errorf("userId value cannot be nil")
	}

	userIdStr := userId.GetStringValue()
	if userIdStr == "" {
		return nil, nil, fmt.Errorf("userId cannot be empty")
	}

	l.Info("disabling user", zap.String("userId", userIdStr))

	err := s.client.UpdateUserActiveStatus(ctx, userIdStr, false)
	if err != nil {
		l.Error("failed to disable user", zap.String("userId", userIdStr), zap.Error(err))
		return nil, nil, fmt.Errorf("failed to disable user %s: %w", userIdStr, err)
	}

	response := &structpb.Struct{
		Fields: map[string]*structpb.Value{
			"success": structpb.NewBoolValue(true),
		},
	}
	return response, nil, nil
}
