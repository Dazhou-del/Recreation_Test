package api

import (
	"common/biz"
	common "common/result"
	"common/rpc"
	"github.com/gin-gonic/gin"
	"user/pb"
	"utils/trance"
)

type InternalCertificationHandler struct {
}

func NewInternalCertificationHandler() *InternalCertificationHandler {
	return &InternalCertificationHandler{}
}

func (i *InternalCertificationHandler) SaveInternalCertification(ctx *gin.Context) {
	var ic pb.SaveInternalCertificationParams
	if err := ctx.ShouldBindJSON(&ic); err != nil {
		common.Fail(ctx, biz.RequestDataError)

		return
	}

	_, err := rpc.InternalCertificationServiceClient.SaveInternalCertification(trance.GetTranceIdCtx(ctx), &ic)
	if err != nil {
		common.Fail(ctx, biz.SaveInternalCertification)

		return
	}

}
