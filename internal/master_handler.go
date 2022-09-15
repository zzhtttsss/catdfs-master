package internal

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
)

var GlobalMasterHandler *MasterHandler

type MasterHandler struct {
	pb.UnimplementedRegisterServiceServer
	pb.UnimplementedHeartbeatServiceServer
	pb.UnimplementedMasterAddServiceServer
}

//CreateMasterHandler 创建MasterHandler
func CreateMasterHandler() {
	config.InitConfig()
	GlobalMasterHandler = &MasterHandler{}
}

// Heartbeat 由Chunkserver调用该方法，维持心跳
func (handler *MasterHandler) Heartbeat(ctx context.Context, args *pb.HeartbeatArgs) (*pb.HeartbeatReply, error) {
	logrus.WithContext(ctx).Infof("[Id=%s] Get heartbeat.", args.Id)
	err := DoHeartbeat(args.Id)
	if err != nil {
		logrus.Errorf("Fail to heartbeat, error code: %v, error detail: %s,", common.MasterHeartbeatFailed, err.Error())
		details, _ := status.New(codes.NotFound, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterHeartbeatFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	rep := &pb.HeartbeatReply{}
	return rep, nil
}

// Register 由Chunkserver调用该方法，将对应DataNode注册到本NameNode上
func (handler *MasterHandler) Register(ctx context.Context, args *pb.DNRegisterArgs) (*pb.DNRegisterReply, error) {
	id, err := DoRegister(ctx)
	if err != nil {
		logrus.Errorf("Fail to register, error code: %v, error detail: %s,", common.MasterRegisterFailed, err.Error())
		details, _ := status.New(codes.NotFound, "").WithDetails(&pb.RPCError{
			Code: common.MasterRegisterFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	rep := &pb.DNRegisterReply{
		Id: id,
	}
	return rep, nil
}

// CheckArgs4Add Called by client.
// Check whether the path and file name entered by the user in the Add operation are legal.
func (handler *MasterHandler) CheckArgs4Add(ctx context.Context, args *pb.CheckArgs4AddArgs) (*pb.CheckArgs4AddReply, error) {
	logrus.WithContext(ctx).Infof("Get request for check add args from client, path: %s, filename: %s, size: %d", args.Path, args.FileName, args.Size)
	fileNodeId, chunkNum, err := DoCheckArgs4Add(args)
	if err != nil {
		logrus.Errorf("Fail to check path and filename for add operation, error code: %v, error detail: %s,", common.MasterCheckArgs4AddFailed, err.Error())
		details, _ := status.New(codes.InvalidArgument, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterCheckArgs4AddFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	rep := &pb.CheckArgs4AddReply{
		FileNodeId: fileNodeId,
		ChunkNum:   chunkNum,
	}
	return rep, nil

}

// GetDataNodes4Add Called by client.
// Allocate some DataNode to store a Chunk and select the primary DataNode
func (handler *MasterHandler) GetDataNodes4Add(ctx context.Context, args *pb.GetDataNodes4AddArgs) (*pb.GetDataNodes4AddReply, error) {
	logrus.WithContext(ctx).Infof("Get request for get dataNodes for single chunk from client, FileNodeId: %s, ChunkIndex: %d", args.FileNodeId, args.ChunkIndex)
	dataNodes, primaryNode, err := DoGetDataNodes4Add(args.FileNodeId, args.ChunkIndex)
	if err != nil {
		logrus.Errorf("Fail to get dataNodes for single chunk for add operation, error code: %v, error detail: %s,", common.MasterGetDataNodes4AddFailed, err.Error())
		details, _ := status.New(codes.InvalidArgument, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterGetDataNodes4AddFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	rep := &pb.GetDataNodes4AddReply{
		DataNodes:   dataNodes,
		PrimaryNode: primaryNode,
	}
	logrus.WithContext(ctx).Infof("Success to get dataNodes for single chunk for add operation, FileNodeId: %s, ChunkIndex: %d", args.FileNodeId, args.ChunkIndex)
	return rep, nil
}

// UnlockDic4Add Called by client.
// Unlock all FileNode in the target path which is used to add file.
func (handler *MasterHandler) UnlockDic4Add(ctx context.Context, args *pb.UnlockDic4AddArgs) (*pb.UnlockDic4AddReply, error) {
	logrus.WithContext(ctx).Infof("Get request for unlock FileNodes in the target path from client, FileNodeId: %s", args.FileNodeId)
	err := DoUnlockDic4Add(args.FileNodeId, false)
	if err != nil {
		logrus.Errorf("Fail to unlock FileNodes in the target path, error code: %v, error detail: %s,", common.MasterUnlockDic4AddFailed, err.Error())
		details, _ := status.New(codes.Internal, err.Error()).WithDetails(&pb.RPCError{
			Code: common.MasterUnlockDic4AddFailed,
			Msg:  err.Error(),
		})
		return nil, details.Err()
	}
	rep := &pb.UnlockDic4AddReply{}
	logrus.WithContext(ctx).Infof("Success to unlock FileNodes in the target path, FileNodeId: %s", args.FileNodeId)
	return rep, nil
}

func (handler *MasterHandler) Server() {
	listener, err := net.Listen(common.TCP, viper.GetString(common.MasterPort))
	if err != nil {
		logrus.Errorf("Fail to server, error code: %v, error detail: %s,", common.MasterRPCServerFailed, err.Error())
		os.Exit(1)
	}
	server := grpc.NewServer()
	pb.RegisterRegisterServiceServer(server, handler)
	pb.RegisterHeartbeatServiceServer(server, handler)
	pb.RegisterMasterAddServiceServer(server, handler)
	logrus.Infof("Master is running, listen on %s%s", common.LocalIP, viper.GetString(common.MasterPort))
	server.Serve(listener)
}
