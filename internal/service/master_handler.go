package service

import (
	"context"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
	pb.UnimplementedHeartbeatServiceServer
}

//CreateMasterHandler 创建MasterHandler
func CreateMasterHandler() {
	config.InitConfig()
	GlobalMasterHandler = &MasterHandler{
		GlobalNameNode: model.CreateNameNode(),
	}
}

// Heartbeat 响应由chunkserver调用的rpc
func (handler *MasterHandler) Heartbeat(ctx context.Context, args *pb.HeartbeatArgs) (*pb.HeartbeatReply, error) {
	logrus.WithContext(ctx).Infof("[Id=%s] Get heartbeat.", args.Id)
	err := handler.GlobalNameNode.Heartbeat(args.Id)
	if err != nil {
		logrus.Errorf("fail to heartbeat, error code: %v, error detail: %s,", common.MasterHeartbeatFailed, err.Error())
		details, _ := status.New(codes.NotFound, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterHeartbeatFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	rep := &pb.HeartbeatReply{}
	return rep, nil

}

// Register 由DataNode调用该方法，将对应DataNode注册到本NameNode上
func (handler *MasterHandler) Register(ctx context.Context, args *pb.DNRegisterArgs) (*pb.DNRegisterReply, error) {
	id, address, err := handler.GlobalNameNode.Register(ctx)
	if err != nil {
		logrus.Errorf("fail to register, error code: %v, error detail: %s,", common.MasterRegisterFailed, err.Error())
		details, _ := status.New(codes.NotFound, "").WithDetails(&pb.RPCError{
			Code: common.MasterRegisterFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
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
		logrus.Errorf("fail to server, error code: %v, error detail: %s,", common.MasterRPCServerFailed, err.Error())
		os.Exit(1)
	}
	server := grpc.NewServer()
	pb.RegisterRegisterServiceServer(server, handler)
	pb.RegisterHeartbeatServiceServer(server, handler)
	logrus.Infof("Master is running, listen on %s%s", common.LocalIP, viper.GetString(common.MasterPort))
	server.Serve(listener)
}
