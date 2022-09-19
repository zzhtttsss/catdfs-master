package internal

import (
	"context"
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"net"
	"os"
	"time"
	"tinydfs-base/common"
	"tinydfs-base/config"
	"tinydfs-base/protocol/pb"
	"tinydfs-master/internal"
)

var GlobalSMHandler *ShadowMasterHandler

type ShadowMasterHandler struct {
	SM *ShadowMaster
	pb.UnimplementedSendOperationServiceServer
}

//CreateSMHandler creates CreateSMHandler to handler rpc
func CreateSMHandler() {
	config.InitConfig()
	sm := CreateShadowMaster()
	GlobalSMHandler = &ShadowMasterHandler{
		SM: sm,
	}
	GlobalSMHandler.SM.flushTimer = time.NewTimer(time.Duration(viper.GetInt(common.SMFsimageFlushTime)) * time.Second)
	go GlobalSMHandler.Flush4Fsimage()

}

func (sm *ShadowMasterHandler) SendOperation(ctx context.Context, args *pb.OperationArgs) (*pb.OperationReply, error) {
	logrus.Infof("Get an operation from master, operation id : %s", args.Uuid)
	err := DoSendOperation(args)
	if err != nil {
		details, _ := status.New(codes.Unknown, err.Error()).WithDetails(&pb.RPCError{
			Code: common.ShadowMasterSendOperationFailed,
			Msg:  err.Error(),
		})
		return &pb.OperationReply{Ok: false}, details.Err()
	}
	return &pb.OperationReply{Ok: true}, nil
}

func (sm *ShadowMasterHandler) FinishOperation(ctx context.Context, args *pb.OperationArgs) (*pb.OperationReply, error) {
	logrus.Infof("Get an finish operation from master, operation id : %s", args.Uuid)
	err := DoFinishOperation(args)
	if err != nil {
		details, _ := status.New(codes.Unknown, err.Error()).WithDetails(&pb.RPCError{
			Code: common.ShadowMasterFinishOperationFailed,
			Msg:  err.Error(),
		})
		return &pb.OperationReply{Ok: false}, details.Err()
	}
	return &pb.OperationReply{Ok: true}, nil
}

func (sm *ShadowMasterHandler) Flush4Fsimage() {
	for {
		select {
		case <-sm.SM.flushTimer.C:
			//TODO
			// Logger should close the edits.txt handle and rename edits.txt to log.
			// At this time, all the operations should be blocked until a new edits.txt is created.
			err := sm.SM.writer.Close()
			if err != nil {
				logrus.Errorf("Close edits.txt failed. Error detail %s\n", err)
			}

			// 1. rename the edits.log and create a new edits.log
			newPath := fmt.Sprintf("log/%s.log", time.Now().Format(common.LogFileTimeFormat))
			err = os.Rename(fmt.Sprintf("%s", common.LogFileName), newPath)
			if err != nil {
				logrus.Errorf("Rename edits.txt failed. Error detail %s\n", err)
				//TODO continue的合理性
				sm.SM.flushTimer.Reset(time.Duration(viper.GetInt(common.SMFsimageFlushTime)) * time.Second)
				continue
			}
			newLogWriter, err := os.OpenFile(common.LogFileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0755)
			if err != nil {
				logrus.Errorf("Create edits.txt failed. Error detail %s\n", err)
			}
			sm.SM.log.SetOutput(newLogWriter)
			sm.SM.writer = newLogWriter
			// 2. read edits.log and merge the operations onto the sm.directory
			ops := internal.ReadLogLines(newPath)
			internal.Merge2Root(sm.SM.shadowRoot, ops)
			// 3. serialize the sm.directory and store it in the fsimage.txt
			internal.RootSerialize(sm.SM.shadowRoot)
			sm.SM.flushTimer.Reset(time.Duration(viper.GetInt(common.SMFsimageFlushTime)) * time.Second)
			logrus.Infof("Success to flush to Fsimage, time: %s", time.Now().String())
		}
	}
}

func (sm *ShadowMasterHandler) Server() {
	listener, err := net.Listen(common.TCP, viper.GetString(common.SMPort))
	if err != nil {
		logrus.Errorf("Fail to server, error code: %v, error detail: %s,", common.ShadowMasterRPCServerFailed, err.Error())
		os.Exit(1)
	}
	server := grpc.NewServer()
	pb.RegisterSendOperationServiceServer(server, sm)
	logrus.Infof("Shadow Master is running, listen on %s%s", common.LocalIP, viper.GetString(common.SMPort))
	server.Serve(listener)
}
