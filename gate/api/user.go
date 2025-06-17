package api

import (
	"common/biz"
	"common/config"
	"go.opentelemetry.io/otel"
	"utils/method"

	jwts "common/jwts"
	"common/logs"
	common "common/result"
	"common/rpc"
	"framework/msError"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"user/pb"
)

type UserHandler struct {
}

func NewUserHandler() *UserHandler {
	return &UserHandler{}
}

var Tracer = otel.Tracer("api1")

func (u *UserHandler) Register(ctx *gin.Context) {
	// 接收参数
	var req pb.RegisterParams
	err := ctx.ShouldBind(&req)
	if err != nil {
		common.Fail(ctx, biz.RequestDataError)

		return
	}

	// 调用user rpc注册用户
	response, err := rpc.UserClient.Register(method.GetTranceIdCtx(ctx), &req)
	if err != nil {
		common.Fail(ctx, msError.ToError(err))

		return
	}

	if len(response.Uid) == 0 {
		common.Fail(ctx, biz.SqlError)

		return
	}

	// 生成token
	token, err := jwts.GenerateToken(response.Uid, config.Conf.Jwt.Exp, config.Conf.Jwt.Secret, response.RoleList)
	if err != nil {
		logs.Log.Error("user register generate err", zap.Error(err))
		common.Fail(ctx, biz.Fail)

		return
	}

	result := map[string]any{
		"token": token,
		"serverInfo": map[string]any{
			"host": config.Conf.Services["connector"].ClientHost,
			"port": config.Conf.Services["connector"].ClientPort,
		},
	}

	common.Success(ctx, result)
}
