package middleware

//
//import (
//	"crmeb_go/define"
//	"crmeb_go/internal/common/session_context"
//	"crmeb_go/internal/data/common/session"
//	"crmeb_go/internal/data/errorm"
//	"crmeb_go/internal/server"
//	"crmeb_go/utils/ihttp"
//	"crmeb_go/utils/ijwt"
//	"errors"
//	"github.com/gin-gonic/gin"
//	"net/http"
//	"strings"
//)
//
//func ApiAuthVisitorMiddleWare(svc *server.SvcContext) gin.HandlerFunc {
//	return func(c *gin.Context) {
//		// 定义不需要认证的路径
//		whitelist := []string{"/api/admin/validate/code", "/api/admin/getLoginPic", "/api/admin/login"}
//
//		// 检查请求路径是否在白名单中
//		for _, path := range whitelist {
//			if strings.HasPrefix(c.Request.URL.Path, path) {
//				c.Next()
//				return
//			}
//		}
//
//		token := c.GetHeader(define.AdminToken)
//		if token == "" {
//			c.JSON(http.StatusOK, ihttp.Error(errors.New("无权限"), errorm.StatusUnSilentAuthorized))
//			c.Abort()
//			return
//		}
//
//		parseToken, err := ijwt.ParseToken(token, svc.Conf.JWT.AccessSecret)
//		if err != nil {
//			c.JSON(http.StatusOK, ihttp.Error(errors.New("无权限"), errorm.StatusUnSilentAuthorized))
//			c.Abort()
//			return
//		}
//
//		// 存放登录信息到会话上下文
//		sessionContext := session_context.GetSessionContext(c)
//		sessionContext.LoginUserInfo = &session.LoginUserInfo{UserId: parseToken.UserId, Roles: parseToken.Roles, ClientIp: c.ClientIP()}
//		c.Set(define.SystemSessionContext, sessionContext)
//
//		c.Next()
//	}
//}
