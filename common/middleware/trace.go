package middleware

import (
	"common/logs"
	"github.com/gin-gonic/gin"
	"github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
	"go.uber.org/zap"
	"io"
)

func Trace() gin.HandlerFunc {
	return func(ctx *gin.Context) {
		//jaeger配置
		cfg := jaegercfg.Configuration{
			Sampler: &jaegercfg.SamplerConfig{
				Type:  jaeger.SamplerTypeConst,
				Param: 1, //全部采样
			},
			Reporter: &jaegercfg.ReporterConfig{
				//当span发送到服务器时要不要打日志
				LogSpans:           true,
				LocalAgentHostPort: "localhost:6831",
			},
			ServiceName: "gin",
		}
		//创建jaeger
		tracer, closer, err := cfg.NewTracer(jaegercfg.Logger(jaeger.StdLogger))
		if err != nil {
			panic(err)
		}
		defer func(closer io.Closer) {
			err := closer.Close()
			if err != nil {
				logs.Log.Error("tracing closer err", zap.Error(err))
			}
		}(closer)
		//最开始的span，以url开始
		startSpan := tracer.StartSpan(ctx.Request.URL.Path)
		defer startSpan.Finish()
		ctx.Set("tracer", tracer)
		ctx.Set("parentSpan", startSpan)
		ctx.Next()
	}
}
