package app

import (
	"common/config"
	"common/discovery"
	"common/logs"
	"context"
	"core/repo"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"net"
	"user/internal/service"
	"user/pb"
)

// Run 启动程序 启动grpc服务 启用http服务  启用日志 启用数据库
func Run(ctx context.Context) error {
	// 初始化日志库
	logs.NewZap(config.Conf.Log)
	// etcd注册中心 grpc服务注册到etcd中 客户端访问的时候 通过etcd获取grpc的地址
	register := discovery.NewRegister()
	// 启动grpc服务端
	server := grpc.NewServer()
	// 注册 grpc service 需要数据库 mongo redis
	// 初始化 数据库管理
	manager := repo.New(ctx)

	go func() {
		lis, err := net.Listen("tcp", config.Conf.Grpc.Addr)
		if err != nil {
			logs.Log.Fatal("user grpc server listen err", zap.Error(err))
		}

		err = register.Register(ctx, config.Conf.Etcd)
		if err != nil {
			logs.Log.Fatal("user grpc server register etcd err", zap.Error(err))
		}

		pb.RegisterUserServiceServer(server, service.NewAccountService(manager))

		// 阻塞操作
		err = server.Serve(lis)
		if err != nil {
			logs.Log.Fatal("user grpc server run failed err", zap.Error(err))
		}
	}()

	return nil
}
