package logs

import (
	"common/config"
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"time"
	"utils/idirectory"
)

const ctxLoggerKey = "zapLogger"

var Log *Logger

type Logger struct {
	*zap.Logger
}

func NewZap(z config.Zap) {
	// 判断是否有Director文件夹 没有就创建
	if ok, _ := idirectory.PathExists(z.Director); !ok {
		fmt.Printf("create %v directory\n", z.Director)
		_ = os.Mkdir(z.Director, os.ModePerm)
	}

	// 据配置文件中的日志级别设置，创建多个core 实例
	levels := z.Levels()
	length := len(levels)
	cores := make([]zapcore.Core, 0, length)

	for i := 0; i < length; i++ {
		core := NewZapCore(&z, levels[i])
		cores = append(cores, core)
	}

	logger := zap.New(zapcore.NewTee(cores...))
	if z.ShowLine {
		logger = logger.WithOptions(zap.AddCaller())
	}

	Log = &Logger{
		logger,
	}
}

func timeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	// enc.AppendString(t.Format("2006-01-02 15:04:05"))
	enc.AppendString(t.Format("2006-01-02 15:04:05.000"))
}

// WithValue Adds a field to the specified context
func (l *Logger) WithValue(ctx context.Context, fields ...zapcore.Field) context.Context {
	if c, ok := ctx.(*gin.Context); ok {
		ctx = c.Request.Context()
		c.Request = c.Request.WithContext(context.WithValue(ctx, ctxLoggerKey, l.WithContext(ctx).With(fields...)))
		return c
	}
	return context.WithValue(ctx, ctxLoggerKey, l.WithContext(ctx).With(fields...))
}

// WithContext Returns a zap instance from the specified context
func (l *Logger) WithContext(ctx context.Context) *Logger {
	if c, ok := ctx.(*gin.Context); ok {
		ctx = c.Request.Context()
	}
	zl := ctx.Value(ctxLoggerKey)
	ctxLogger, ok := zl.(*zap.Logger)
	if ok {
		return &Logger{ctxLogger}
	}
	return l
}
