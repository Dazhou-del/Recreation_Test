package middleware

import (
	"common/config"
	"github.com/gin-gonic/gin"
	"net/http"
)

type CorsH struct {
}

func NewCorsH() *CorsH {
	return &CorsH{}
}

// CorsHandler 直接放行所有跨域请求并放行所有 OPTIONS 方法
func (c *CorsH) CorsHandler() gin.HandlerFunc {
	return func(c *gin.Context) {
		method := c.Request.Method
		origin := c.Request.Header.Get("Origin")
		c.Writer.Header().Set("Access-Control-Allow-Origin", origin)

		c.Header("Access-Control-Allow-Origin", origin)
		c.Header("Access-Control-Allow-Headers", "Content-Type,AccessToken,X-CSRF-Token, Authorization, Token,X-Token,X-User-Id")
		c.Header("Access-Control-Allow-Methods", "POST, GET, OPTIONS,DELETE,PUT")
		c.Header("Access-Control-Expose-Headers", "Content-Length, Access-Control-Allow-Origin, Access-Control-Allow-Headers, Content-Type, New-Token, New-Expires-At")
		c.Header("Access-Control-Allow-Credentials", "true")

		// 放行所有OPTIONS方法
		if method == "OPTIONS" {
			c.Header("Access-Control-Allow-Methods", c.GetHeader("Access-Control-Request-Method"))
			c.Header("Access-Control-Allow-Headers", c.GetHeader("Access-Control-Request-Headers"))
			c.Header("Access-Control-Max-Age", "7200")
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		// 处理请求
		c.Next()
	}
}

// CorsByRulesHandler 按照配置处理跨域请求
func (c *CorsH) CorsByRulesHandler() gin.HandlerFunc {
	// 放行全部
	if config.Conf.Cors.Mode == "allow-all" {
		return c.CorsHandler()
	}
	return func(c *gin.Context) {
		whitelist := checkCors(c.GetHeader("origin"))

		// 通过检查, 添加请求头
		if whitelist != nil {
			c.Header("Access-Control-Allow-Origin", whitelist.AllowOrigin)
			c.Header("Access-Control-Allow-Headers", whitelist.AllowHeaders)
			c.Header("Access-Control-Allow-Methods", whitelist.AllowMethods)
			c.Header("Access-Control-Expose-Headers", whitelist.ExposeHeaders)
			if whitelist.AllowCredentials {
				c.Header("Access-Control-Allow-Credentials", "true")
			}
		}

		// 严格白名单模式且未通过检查，直接拒绝处理请求
		if whitelist == nil && config.Conf.Cors.Mode == "strict-whitelist" && (c.Request.Method != "GET" || c.Request.URL.Path != "/health") {
			c.AbortWithStatus(http.StatusForbidden)
		} else {
			// 非严格白名单模式，无论是否通过检查均放行所有 OPTIONS 方法
			if c.Request.Method == http.MethodOptions {
				c.AbortWithStatus(http.StatusNoContent)
			}
		}

		// 处理请求
		c.Next()
	}
}

func checkCors(currentOrigin string) *config.CORSWhitelist {
	for _, whitelist := range config.Conf.Cors.Whitelist {
		// 遍历配置中的跨域头，寻找匹配项
		if currentOrigin == whitelist.AllowOrigin {
			return &whitelist
		}
	}
	return nil
}
