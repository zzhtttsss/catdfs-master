package service

import (
	"context"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"net"
	"os"
	"tinydfs-base/common"
	"tinydfs-base/config"
	"tinydfs-base/protocol/pb"
	"tinydfs-master/internal/model"
)

var GlobalMasterHandler *MasterHandler

type MasterHandler struct {
	GlobalNameNode *model.NameNode
	pb.UnimplementedRegisterServiceServer
}

//CreateMasterHandler 创建MasterHandler
func CreateMasterHandler() {
	config.InitConfig()
	GlobalMasterHandler = &MasterHandler{
		GlobalNameNode: model.CreateNameNode(),
	}
}

// Register 由DataNode调用该方法，将对应DataNode注册到本NameNode上
func (handler *MasterHandler) Register(ctx context.Context, args *pb.DNRegisterArgs) (*pb.DNRegisterReply, error) {
	logrus.WithContext(ctx).Info("receive register request")
	id, address, err := handler.GlobalNameNode.Register(ctx)
	if err != nil {
		logrus.Errorf("fail to register, error code: %v, error detail: %s,", 3301, err.Error())
		return nil, err
	}
	rep := &pb.DNRegisterReply{
		Id:   id,
		Addr: address,
	}
	return rep, nil
}

func (handler *MasterHandler) Server() {
	listener, err := net.Listen(common.TCP, viper.GetString(common.MasterPort))
	if err != nil {
		logrus.Errorf("fail to server, error code: %v, error detail: %s,", 3301, err.Error())
		os.Exit(1)
	}
	server := grpc.NewServer()
	pb.RegisterRegisterServiceServer(server, handler)
	logrus.Infof("NameNode is running, listen on %s%s", common.LocalIP, common.MasterPort)
	server.Serve(listener)
}