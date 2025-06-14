package method

import (
	"common/biz"
	"common/enum"
	"common/logs"
	common "common/result"
	"context"
	"github.com/gin-gonic/gin"
	"google.golang.org/grpc/metadata"
)

// GetTranceIdCtx 获取带traceId的ctx
func GetTranceIdCtx(ctx *gin.Context) context.Context {
	traceId, exists := ctx.Get(enum.TraceId)
	if !exists {
		logs.Log.WithContext(ctx).Error("traceId not exists")
		common.Fail(ctx, biz.GetTraceIdCtx)

		return ctx
	}

	return metadata.NewOutgoingContext(ctx, metadata.Pairs(enum.TraceId, traceId.(string)))
}
