package dao

import (
	"common/logs"
	"context"
	"core/models/entity"
	"core/repo"
	"errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.uber.org/zap"
)

type AccountDao struct {
	repo *repo.Manager
}

func (d *AccountDao) SaveAccount(ctx context.Context, ac *entity.Account) error {
	collection := d.repo.Mongo.Db.Collection("account")

	_, err := collection.InsertOne(ctx, ac)
	if err != nil {
		logs.Log.WithContext(ctx).Error("SaveAccount fail err:", zap.Error(err), zap.Any("ac", ac))

		return err
	}
	return nil
}

func (d *AccountDao) FindAccount(ctx context.Context, account string) (*entity.Account, error) {
	db := d.repo.Mongo.Db.Collection("account")
	result := db.FindOne(ctx, bson.D{{Key: "account", Value: account}})

	ac := new(entity.Account)
	err := result.Decode(ac)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, err
		}
	}

	return ac, nil
}

func NewAccountDao(m *repo.Manager) *AccountDao {
	return &AccountDao{
		repo: m,
	}
}
