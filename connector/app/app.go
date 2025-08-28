package app

import (
	"common/config"
	"common/logs"
	"connector/route"
	"context"
	"core/repo"
	"framework/connector"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Run 启动程序 启动grpc服务 启用http服务  启用日志 启用数据库
func Run(ctx context.Context) error {
	// 初始化日志库
	logs.NewZap(config.Conf.Log)

	exit := func() {}
	go func() {
		c := connector.Default()
		exit = c.Close
		// 初始化数据库管理
		manager := repo.New(ctx)
		c.RegisterHandler(route.Register(manager))
		// todo serverId 后续在启动服务时通过参数传入
		c.Run("connector-001", config.Conf.Server.MaxConn)
	}()

	stop := func() {
		//other
		exit()
		time.Sleep(3 * time.Second)
		logs.Log.Info("stop app finish")
	}

	//期望有一个优雅启停 遇到中断 退出 终止 挂断
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT, syscall.SIGHUP)
	for {
		select {
		case <-ctx.Done():
			stop()
			//time out
			return nil
		case s := <-c:
			switch s {
			case syscall.SIGTERM, syscall.SIGQUIT, syscall.SIGINT:
				stop()
				logs.Log.Info("connector app quit")
				return nil
			case syscall.SIGHUP:
				stop()
				logs.Log.Info("hang up!! connector app quit")
				return nil
			default:
				return nil
			}
		}
	}
}
