package router

import (
	"common/config"
	middleware "common/middleware"
	"common/rpc"
	"gate/api"
	"gate/enum"
	"github.com/gin-gonic/gin"
)

// RegisterRouter 路由注册
func RegisterRouter() *gin.Engine {
	if config.Conf.Log.Level == enum.LogDebugLevel {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	// 初始化grpc的client gate是做为grpc的客户端 去调用user rpc服务
	rpc.Init()

	r := gin.Default()
	// CorsByRules 按照配置的规则放行跨域请求
	r.Use(middleware.CorsByRules())
	// 日志中间件
	r.Use(middleware.NewLogM().RequestLogMiddleware())
	// 注册用户接口
	userHandler := api.NewUserHandler()
	r.POST("/register", userHandler.Register)

	return r
}
