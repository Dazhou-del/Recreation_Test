package jwts

import (
	"common/enum"
	"fmt"
	"github.com/golang-jwt/jwt/v5"
)

type TokenData struct {
	UserId   string
	ExpireAt int64
	RoleList []string
}

// ParseToken 解析token
func ParseToken(token string, secretKey string) (*TokenData, error) {
	newToken, err := jwt.Parse(token, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return []byte(secretKey), nil
	})

	if err != nil {
		return nil, err
	}

	claims, ok := newToken.Claims.(jwt.MapClaims)
	if !ok || !newToken.Valid {
		return nil, fmt.Errorf("invalid token")
	}

	userId, ok := claims[enum.TokenUserId].(string)
	if !ok {
		return nil, fmt.Errorf("invalid user_id type")
	}

	expireAt, ok := claims[enum.TokenExpireAt].(float64)
	if !ok {
		return nil, fmt.Errorf("invalid expire_at type")
	}

	roleList, ok := claims[enum.TokenRoles].([]string)
	if !ok {
		return nil, fmt.Errorf("invalid expire_at type")
	}

	return &TokenData{
		UserId:   userId,
		ExpireAt: int64(expireAt),
		RoleList: roleList,
	}, nil
}

// GenerateToken 生成token
func GenerateToken(userId string, expiration int64, secretKey string, roles []string) (string, error) {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		enum.TokenUserId:   userId,
		enum.TokenExpireAt: expiration,
		enum.TokenRoles:    roles,
	})

	tokenString, err := token.SignedString([]byte(secretKey))
	if err != nil {
		return "", err
	}
	return tokenString, nil
}
