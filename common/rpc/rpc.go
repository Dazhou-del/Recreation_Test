package rpc

import (
	"common/config"
	"common/discovery"
	"common/logs"
	"context"
	"fmt"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"user/pb"
)

var (
	UserClient                         pb.UserServiceClient
	InternalCertificationServiceClient pb.InternalCertificationServiceClient
)

type Authentication struct {
	clientId     string
	clientSecret string
}

func NewAuthentication(clientId, clientSecret string) *Authentication {
	return &Authentication{
		clientId:     clientId,
		clientSecret: clientSecret,
	}
}

// withClientCredentials
func (a *Authentication) withClientCredentials(clientId, clientSecret string) {
	a.clientId = clientId
	a.clientSecret = clientSecret
}

// GetRequestMetadata 从meta中获取凭证信息
func (a *Authentication) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{
		"client_id":     a.clientId,
		"client_secret": a.clientSecret,
	}, nil
}

// RequireTransportSecurity 指示凭据是否需要传输安全性。
func (a *Authentication) RequireTransportSecurity() bool {
	return false
}

func Init() {
	// etcd解析器 就可以grpc连接的时候 进行触，通过提供的地址 去etcd中寻找
	r := discovery.NewResolver(config.Conf.Etcd)
	resolver.Register(r)
	userDomain := config.Conf.Domain["user"]

	initClient(userDomain.Name, userDomain.LoadBalance, &UserClient)
	initClient(userDomain.Name, userDomain.LoadBalance, &InternalCertificationServiceClient)
}

func initClient(name string, loadBalance bool, client interface{}) {
	// 找到服务的地址
	addr := fmt.Sprintf("etcd:///%s", name)

	// 传输层不需要TLS，禁用安全传输
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithPerRPCCredentials(NewAuthentication(config.Conf.Grpc.ClientId, config.Conf.Grpc.ClientSecret)),
	}

	// 如果启用了负载均衡 (loadBalance)，则在选项中添加了默认服务配置，设置负载均衡策略为 "round_robin"（轮询算法）
	if loadBalance {
		opts = append(opts, grpc.WithDefaultServiceConfig(fmt.Sprintf(`{"LoadBalancingPolicy": "%s"}`, "round_robin")))
	}

	// 连接server
	conn, err := grpc.NewClient(addr, opts...)
	if err != nil {
		logs.Log.Fatal("rpc connect etcd err", zap.Error(err))
	}

	switch c := client.(type) {
	case *pb.UserServiceClient:
		*c = pb.NewUserServiceClient(conn)
	case *pb.InternalCertificationServiceClient:
		*c = pb.NewInternalCertificationServiceClient(conn)
	default:
		logs.Log.Fatal("unsupported client type")
	}
}
