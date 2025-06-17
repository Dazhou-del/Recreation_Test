package tracing

import (
	"fmt"
	"github.com/opentracing/opentracing-go"
	jaeger "github.com/uber/jaeger-client-go"
	jaegercfg "github.com/uber/jaeger-client-go/config"
	jaegerlog "github.com/uber/jaeger-client-go/log"
	"io"
	"time"
)

func init() {
	//os.Setenv("JAEGER_AGENT_HOST", "192.168.1.101")
	//os.Setenv("JAEGER_AGENT_PORT", "6831")
}

// Init returns an instance of Jaeger Tracer that samples 100% of traces and logs all spans to stdout.
func Init(service string) (opentracing.Tracer, io.Closer) {
	var cfg = jaegercfg.Configuration{
		ServiceName: service, // 服务名字
		Sampler: &jaegercfg.SamplerConfig{
			Type:  jaeger.SamplerTypeConst,
			Param: 1, // 所有的服务都被采样
		},
		Reporter: &jaegercfg.ReporterConfig{
			LogSpans: true,
			// 按实际情况替换你的 ip
			CollectorEndpoint:   "http://127.0.0.1:14268/api/traces",
			BufferFlushInterval: 5 * time.Second, // 5s提交一次traces
		},
	}

	// 根据上面的配置创建一个新的tracer
	jLogger := jaegerlog.StdLogger
	tracer, closer, err := cfg.NewTracer(
		jaegercfg.Logger(jLogger),
	)

	if err != nil {
		panic(fmt.Sprintf("ERROR: cannot init Jaeger: %v\n", err))
	}

	return tracer, closer
}
