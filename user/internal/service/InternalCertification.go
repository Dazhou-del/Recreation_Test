package service

import (
	"context"
	"core/dao"
	"core/models/entity"
	"core/repo"
	"time"
	"user/pb"
	"utils/md5"
)

type InternalCertificationService struct {
	internalCertificationDao *dao.InternalCertificationDao
	pb.UnsafeInternalCertificationServiceServer
}

func NewInternalCertificationService(manager *repo.Manager) *InternalCertificationService {
	return &InternalCertificationService{
		internalCertificationDao: dao.NewInternalCertificationDao(manager),
	}
}

func (i *InternalCertificationService) SaveInternalCertification(ctx context.Context, req *pb.SaveInternalCertificationParams) (*pb.SaveInternalCertificationResponse, error) {
	clientSecret := md5.BcryptHash(req.ClientID)

	ic := &entity.InternalCertification{
		ClientId:       req.ClientID,
		ClientSecret:   clientSecret,
		ExpirationTime: time.Now().Add(time.Hour * 24 * 1000),
	}

	if err := i.internalCertificationDao.SaveInternalCertification(ctx, ic); err != nil {
		return nil, err
	}

	return &pb.SaveInternalCertificationResponse{}, nil
}
