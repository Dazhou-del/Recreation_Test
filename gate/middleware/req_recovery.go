package middleware

import (
	"crmeb_go/utils/ierror"
	"crmeb_go/utils/izap"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"net"
	"net/http"
	"net/http/httputil"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Fn is a function to get zap fields from gin.Context
type Fn func(c *gin.Context) []zapcore.Field

// Skipper is a function to skip logs based on provided Context
type Skipper func(c *gin.Context) bool

// ZapLogger is the minimal logger interface compatible with zap.Logger
type ZapLogger interface {
	Info(msg string, fields ...zap.Field)
	Error(msg string, fields ...zap.Field)
}

// Config is config setting for Ginzap
type Config struct {
	TimeFormat      string
	UTC             bool
	SkipPaths       []string
	SkipPathRegexps []*regexp.Regexp
	Context         Fn
	DefaultLevel    zapcore.Level
	// skip is a Skipper that indicates which logs should not be written.
	// Optional.
	Skipper Skipper
}

// GinZap 返回一个使用 uber-go/zap 记录请求的 gin.HandlerFunc（中间件）。
//
// 带有错误的请求使用 zap.Error() 记录。
// 没有错误的请求使用 zap.Info() 记录。
//
// 它接收以下参数：
//  1. 一个 time 包的时间格式字符串（例如 time.RFC3339）。
//  2. 一个布尔值，表示是否使用 UTC 时区或本地时区。
func GinZap(logger ZapLogger, timeFormat string, utc bool) gin.HandlerFunc {
	return GinZapWithConfig(logger, &Config{TimeFormat: timeFormat, UTC: utc, DefaultLevel: zapcore.InfoLevel})
}

// GinZapWithConfig 返回一个使用 uber-go/zap 记录请求的 gin.HandlerFunc（中间件）。
//
// 带有错误的请求使用 zap.Error() 记录。
// 没有错误的请求使用 zap.Info() 记录。
//
// 它接收一个 Config 结构体和一个 ZapLogger。
// Config 结构体允许你配置日志格式、时间格式和 UTC 时区。
// ZapLogger 是与 zap.Logger 兼容的最小日志记录接口。
func GinZapWithConfig(logger ZapLogger, conf *Config) gin.HandlerFunc {
	skipPaths := make(map[string]bool, len(conf.SkipPaths))
	for _, path := range conf.SkipPaths {
		skipPaths[path] = true
	}

	return func(c *gin.Context) {
		start := time.Now()
		// some evil middlewares modify this values
		path := c.Request.URL.Path
		query := c.Request.URL.RawQuery
		c.Next()
		track := true

		if _, ok := skipPaths[path]; ok || (conf.Skipper != nil && conf.Skipper(c)) {
			track = false
		}

		if track && len(conf.SkipPathRegexps) > 0 {
			for _, reg := range conf.SkipPathRegexps {
				if !reg.MatchString(path) {
					continue
				}

				track = false
				break
			}
		}

		if track {
			end := time.Now()
			latency := end.Sub(start)
			if conf.UTC {
				end = end.UTC()
			}

			trace := uuid.NewString()

			logger.WithValue(c, zap.String("trace", trace))

			fields := []zapcore.Field{
				zap.String("trace", trace),
				zap.Int("status", c.Writer.Status()),
				zap.String("method", c.Request.Method),
				zap.String("path", path),
				zap.String("query", query),
				zap.String("ip", c.ClientIP()),
				zap.String("user-agent", c.Request.UserAgent()),
				zap.Duration("latency", latency),
			}

			if conf.TimeFormat != "" {
				//  暂时不用
				//fields = append(fields, zap.String("time", end.Format(conf.TimeFormat)))
			}

			if conf.Context != nil {
				fields = append(fields, conf.Context(c)...)
			}

			if len(c.Errors) > 0 {
				// Append error field if this is an erroneous request.
				for _, e := range c.Errors.Errors() {
					logger.Error(e, fields...)
				}
			} else {
				if zl, ok := logger.(*zap.Logger); ok {
					zl.Log(conf.DefaultLevel, path, fields...)
				} else if conf.DefaultLevel == zapcore.InfoLevel {
					logger.Info(path, fields...)
				} else {
					logger.Error(path, fields...)
				}
			}
		}
	}
}

func defaultHandleRecovery(c *gin.Context, err interface{}) {
	c.AbortWithStatus(http.StatusInternalServerError)
}

// RecoveryWithZap 返回一个 gin.HandlerFunc（中间件）
// 该中间件可以从任何 panic 中恢复，并使用 uber-go/zap 记录请求。
// 所有错误都使用 zap.Error() 记录。
// stack 参数表示是否输出堆栈信息。
// 堆栈信息有助于找到错误发生的位置，但堆栈信息通常很大。
func RecoveryWithZap(logger ZapLogger, stack bool) gin.HandlerFunc {
	return CustomRecoveryWithZap(logger, stack, defaultHandleRecovery)
}

// CustomRecoveryWithZap 返回一个带有自定义恢复处理程序的 gin.HandlerFunc（中间件）
// 该中间件可以从任何 panic 中恢复，并使用 uber-go/zap 记录请求。
// 所有错误都使用 zap.Error() 记录。
// stack 参数表示是否输出堆栈信息。
// 堆栈信息有助于找到错误发生的位置，但堆栈信息通常很大。
func CustomRecoveryWithZap(logger ZapLogger, stack bool, recovery gin.RecoveryFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		defer func() {
			if err := recover(); err != nil {
				// Check for a broken connection, as it is not really a
				// condition that warrants a panic stack trace.
				var brokenPipe bool
				if ne, ok := err.(*net.OpError); ok {
					var se *os.SyscallError
					if errors.As(ne.Err, &se) {
						if strings.Contains(strings.ToLower(se.Error()), "broken pipe") ||
							strings.Contains(strings.ToLower(se.Error()), "connection reset by peer") {
							brokenPipe = true
						}
					}
				}

				httpRequest, _ := httputil.DumpRequest(c.Request, false)
				if brokenPipe {
					logger.Error(c.Request.URL.Path,
						zap.Any("error", err),
						zap.String("request", string(httpRequest)),
					)
					// If the connection is dead, we can't write a status to it.
					c.Error(err.(error)) //nolint: errcheck
					c.Abort()
					return
				}

				if stack {
					logger.Error("[Recovery from panic]",
						zap.Any("error", err),
						zap.String("request", string(httpRequest)),
						zap.StackSkip("stack", 0), // 添加堆栈信息，跳过指定的调用层级
					)
				} else {
					logger.Error("[Recovery from panic]",
						zap.Any("error", err),
						zap.String("request", string(httpRequest)),
					)
				}

				unmarshalError := ierror.UnmarshalError(errors.New("internal server error, please try again later"))
				buf, err := json.Marshal(unmarshalError)
				if err != nil {
					panic(err)
				}

				c.Data(http.StatusInternalServerError, "application/json; charset=utf-8", buf)
				c.Abort()
			}
		}()
		c.Next()
	}
}
