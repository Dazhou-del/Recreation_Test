package handler

import (
	"common/biz"
	"common/config"
	"common/jwts"
	"common/logs"
	"common/response"
	"connector/models/request"
	"context"
	"core/repo"
	"core/service"
	"encoding/json"
	"framework/game"
	"framework/net"
	"go.uber.org/zap"
)

type EntryHandler struct {
	userService  *service.UserService
	redisService *service.RedisService
}

func (h *EntryHandler) Entry(session *net.Session, body []byte) (any, error) {
	logs.Log.Info("==============Entry Start=====================")
	logs.Log.Info("entry request params:", zap.Any("body", string(body)))
	logs.Log.Info("==============Entry End=====================")

	var req request.EntryReq
	err := json.Unmarshal(body, &req)
	if err != nil {
		return response.F(biz.RequestDataError), nil
	}

	//校验token
	tokenData, err := jwts.ParseToken(req.Token, config.Conf.Jwt.Secret)
	if err != nil {
		logs.Log.Error("parse token", zap.Error(err))

		return response.F(biz.TokenInfoError), nil
	}

	//根据uid 去mongo中查询用户 如果用户不存在 生成一个用户
	user, err := h.userService.FindAndSaveUserByUid(context.TODO(), tokenData.UserId, req.UserInfo)
	if err != nil {
		return response.F(biz.SqlError), nil
	}

	session.Uid = tokenData.UserId
	// 检查帐号冻结
	if user.IsBlockedAccount {
		return response.F(biz.BlockedAccount), nil
	}

	//设置用户可以创建联盟
	user.IsAgent = true
	return response.S(map[string]any{
		"userInfo": user,
		"config":   game.Conf.GetFrontGameConfig(),
	}), nil
}

func NewEntryHandler(r *repo.Manager) *EntryHandler {
	return &EntryHandler{
		userService:  service.NewUserService(r),
		redisService: service.NewRedisService(r),
	}
}
