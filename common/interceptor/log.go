package interceptor

import (
	"common/enum"
	"common/logs"
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type grpcLinkTrackingLog struct {
}

// 构造并初始化 grpc author
func newGrpcLinkTrackingLog() *grpcLinkTrackingLog {
	return &grpcLinkTrackingLog{}
}

// Log 日志拦截器 链路获取客户端的traceId
func (a *grpcLinkTrackingLog) Log(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (resp interface{}, err error) {
	// 从上下文中获取认证信息
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, fmt.Errorf("ctx is not an grpc incoming service")
	}

	data := md.Get(enum.TraceId)
	traceId := data[0]

	// 设置traceId
	ctx = logs.Log.WithTraceId(ctx, traceId)

	resp, err = handler(ctx, req)
	return resp, err
}

func GrpcLogUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return newGrpcLinkTrackingLog().Log
}
