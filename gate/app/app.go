package app

import (
	"common/config"
	"common/logs"
	"context"
	"fmt"
	"gate/router"
	"go.uber.org/zap"
)

// Run 启动程序 启动grpc服务 启用http服务  启用日志 启用数据库
func Run(ctx context.Context) error {
	// 初始化日志库
	logs.NewZap(config.Conf.Log)

	go func() {
		// gin 启动  注册一个路由
		r := router.RegisterRouter()
		// http接口
		if err := r.Run(fmt.Sprintf(":%d", config.Conf.HttpPort)); err != nil {
			logs.Log.Fatal("gate gin run err", zap.Error(err))
		}

		fmt.Println("http server run", "localhost:", config.Conf.HttpPort)
	}()

	return nil
}
