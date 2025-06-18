package entity

import (
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

type InternalCertification struct {
	Id             primitive.ObjectID `bson:"_id,omitempty"`
	ClientId       string             `bson:"clientId"`       // 客户端id
	ClientSecret   string             `bson:"clientSecret"`   // 客户端密钥
	ExpirationTime time.Time          `bson:"expirationTime"` // 过期时间
}
