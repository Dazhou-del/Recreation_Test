package interceptor

import (
	"context"
	"core/dao"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"time"
	"user/enum"
)

type grpcAuthor struct {
	InternalCertificationDao *dao.InternalCertificationDao
}

// 构造并初始化 grpc author
func newGrpcAuthor(internalCertificationDao *dao.InternalCertificationDao) *grpcAuthor {
	return &grpcAuthor{
		InternalCertificationDao: internalCertificationDao,
	}
}

// GetClientCredentialsFromMeta 从客户端发来的请求中获取凭证信息
func (g *grpcAuthor) GetClientCredentialsFromMeta(md metadata.MD) (clientId, clientSecret string) {
	cis := md.Get(enum.ClientHeaderKey)
	sids := md.Get(enum.ClientSecretKey)
	if len(cis) > 0 {
		clientId = cis[0]
	}

	if len(sids) > 0 {
		clientSecret = sids[0]
	}

	return
}

// 验证凭证信息
func (g *grpcAuthor) validateServiceCredential(ctx context.Context, clientId, clientSecret string) error {
	if clientId == "" && clientSecret == "" {
		return status.Errorf(codes.Unauthenticated, "client_id or client_secret is invalidate")
	}

	ic, err := g.InternalCertificationDao.FindInternalCertificationByClientId(ctx, clientId)
	if err != nil {
		return status.Errorf(codes.Unauthenticated, "client_id or client_secret invalidate")
	}

	if clientId != ic.ClientId || clientSecret != ic.ClientSecret || time.Now().Before(ic.ExpirationTime) {
		return status.Errorf(codes.Unauthenticated, "client_id or client_secret invalidate")
	}

	return nil
}

// Auth 普通模式的拦截器
func (g *grpcAuthor) Auth(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (resp interface{}, err error) {
	if info.FullMethod == "/InternalCertificationService/SaveInternalCertification" {
		return handler(ctx, req)
	}

	// 从上下文中获取认证信息
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, fmt.Errorf("ctx is not an grpc incoming service")
	}

	// 获取客户端凭证信息
	clientId, clientSecret := g.GetClientCredentialsFromMeta(md)

	// 校验调用的客户端携带的凭证是否有效
	if err := g.validateServiceCredential(ctx, clientId, clientSecret); err != nil {
		return nil, err
	}

	resp, err = handler(ctx, req)
	return resp, err
}

// StreamAuth 流模式的拦截器
func (g *grpcAuthor) StreamAuth(
	srv interface{},
	ss grpc.ServerStream,
	info *grpc.StreamServerInfo,
	handler grpc.StreamHandler,
) (err error) {
	// 从上下文中获取认证信息
	md, ok := metadata.FromIncomingContext(ss.Context())
	if !ok {
		return fmt.Errorf("ctx is not an grpc incoming service")
	}

	// 获取客户端凭证
	clientId, clientSecret := g.GetClientCredentialsFromMeta(md)

	// 校验调用的客户端凭证是否有效
	if err := g.validateServiceCredential(ss.Context(), clientId, clientSecret); err != nil {
		return err
	}

	return handler(srv, ss)
}

func GrpcAuthUnaryServerInterceptor(internalCertificationDao *dao.InternalCertificationDao) grpc.UnaryServerInterceptor {
	return newGrpcAuthor(internalCertificationDao).Auth
}

func GrpcAuthStreamServerInterceptor(internalCertificationDao *dao.InternalCertificationDao) grpc.StreamServerInterceptor {
	return newGrpcAuthor(internalCertificationDao).StreamAuth
}
