package middleware

import (
	"crmeb_go/define"
	"crmeb_go/internal/common/session_context"
	"github.com/gin-gonic/gin"
)

func SessionMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		// 存放登录信息到上下文
		sessionContext := session_context.New(c)
		c.Set(define.SystemSessionContext, sessionContext)
		c.Next()
	}
}
