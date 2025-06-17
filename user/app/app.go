package app

import (
	"common/config"
	"common/discovery"
	"common/interceptor"
	"common/logs"
	"context"
	"core/repo"
	"user/internal/service"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"net"
	"user/pb"
)

// Run 启动程序 启动grpc服务 启用http服务  启用日志 启用数据库
func Run(ctx context.Context) error {
	// 初始化日志库
	logs.NewZap(config.Conf.Log)

	// 初始化 gRPC 服务
	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			interceptor.GrpcAuthUnaryServerInterceptor(),
			interceptor.GrpcLogUnaryServerInterceptor(),
		),
		grpc.ChainStreamInterceptor(
			interceptor.GrpcAuthStreamServerInterceptor(),
		),
	)

	// 初始化数据库管理
	manager := repo.New(ctx)

	// 启动 gRPC 服务监听协程
	go func() {
		// gRPC监听端口
		lis, err := net.Listen("tcp", config.Conf.Grpc.Addr)
		if err != nil {
			logs.Log.Fatal("user grpc server listen err", zap.Error(err))
		}

		// 注册服务到 etcd
		register := discovery.NewRegister()
		if err := register.Register(ctx, config.Conf.Etcd); err != nil {
			logs.Log.Fatal("user grpc server register etcd err", zap.Error(err))
		}

		// 注册 gRPC 服务实现
		pb.RegisterUserServiceServer(server, service.NewAccountService(manager))

		// 启动 gRPC 服务
		if err := server.Serve(lis); err != nil {
			logs.Log.Fatal("user grpc server run failed err", zap.Error(err))
		}
	}()

	return nil
}
