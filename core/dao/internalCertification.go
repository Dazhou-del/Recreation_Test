package dao

import (
	"common/logs"
	"context"
	"core/models/entity"
	"core/repo"
	"go.mongodb.org/mongo-driver/bson"
	"go.uber.org/zap"
)

type InternalCertificationDao struct {
	repo *repo.Manager
}

func NewInternalCertificationDao(repo *repo.Manager) *InternalCertificationDao {
	return &InternalCertificationDao{
		repo: repo,
	}
}

func (i *InternalCertificationDao) SaveInternalCertification(ctx context.Context, ic *entity.InternalCertification) error {
	collection := i.repo.Mongo.Db.Collection("internalCertification")

	_, err := collection.InsertOne(ctx, ic)
	if err != nil {
		logs.Log.WithContext(ctx).Error("SaveAccount fail err:", zap.Error(err), zap.Any("ac", ic))

		return err
	}

	return nil
}

func (i *InternalCertificationDao) FindInternalCertificationByClientId(ctx context.Context, clientId string) (ic *entity.InternalCertification, err error) {
	collection := i.repo.Mongo.Db.Collection("internalCertification")

	result := collection.FindOne(ctx, &bson.D{
		{Key: "clientId", Value: clientId},
	})

	internalCertification := new(entity.InternalCertification)
	if err := result.Decode(internalCertification); err != nil {
		logs.Log.WithContext(ctx).Error("SaveAccount fail err:", zap.Error(err), zap.String("clientId", clientId))

		return nil, err
	}

	return internalCertification, nil
}
