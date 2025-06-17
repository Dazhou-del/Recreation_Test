package app

import (
	"common/config"
	"common/discovery"
	"common/interceptor"
	"common/logs"
	"common/tracing"
	"context"
	"core/repo"
	grpc_opentracing "github.com/grpc-ecosystem/go-grpc-middleware/tracing/opentracing"
	"github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"io"
	"net"
	"user/internal/service"
	"user/pb"
)

// Run 启动程序 启动grpc服务 启用http服务  启用日志 启用数据库
func Run(ctx context.Context) error {
	// 初始化日志库
	logs.NewZap(config.Conf.Log)

	tracer, closer := tracing.Init("cartService")
	defer func(closer io.Closer) {
		err := closer.Close()
		if err != nil {
			logs.Log.Error("tracing closer err", zap.Error(err))
		}
	}(closer)
	opentracing.SetGlobalTracer(tracer)

	// 启动grpc服务端
	server := grpc.NewServer(
		// 添加验证中间件，校验服务调用的安全信息
		// 如果需要添加多个则使用ChainUnaryInterceptor
		grpc.ChainUnaryInterceptor(interceptor.GrpcAuthUnaryServerInterceptor(), interceptor.GrpcLogUnaryServerInterceptor(),
			grpc_opentracing.UnaryServerInterceptor()), // 链路追踪拦截器

		// 添加Stream API的拦截器
		grpc.StreamInterceptor(interceptor.GrpcAuthStreamServerInterceptor()),
	)

	// 注册 grpc service 需要数据库 mongo redis
	// 初始化 数据库管理
	manager := repo.New(ctx)

	go func() {
		// grpc监听端口
		lis, err := net.Listen("tcp", config.Conf.Grpc.Addr)
		if err != nil {
			logs.Log.Fatal("user grpc server listen err", zap.Error(err))
		}

		// etcd注册中心 grpc服务注册到etcd中 客户端访问的时候 通过etcd获取grpc的地址
		register := discovery.NewRegister()

		// 注册etcd
		err = register.Register(ctx, config.Conf.Etcd)
		if err != nil {
			logs.Log.Fatal("user grpc server register etcd err", zap.Error(err))
		}

		// 注册grpc方法
		pb.RegisterUserServiceServer(server, service.NewAccountService(manager))

		// 启动服务
		err = server.Serve(lis)
		if err != nil {
			logs.Log.Fatal("user grpc server run failed err", zap.Error(err))
		}
	}()

	return nil
}
