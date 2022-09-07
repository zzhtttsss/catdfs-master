package internal

import (
	"context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"tinydfs-base/common"
	"tinydfs-base/config"
	"tinydfs-base/protocol/pb"
	"tinydfs-master/internal"
)

var GlobalSMHandler *ShadowMasterHandler

type ShadowMasterHandler struct {
	ShadowMaster
	pb.UnimplementedSendOperationServiceServer
}

//CreateSMHandler creates CreateSMHandler to handler rpc
func CreateSMHandler() {
	config.InitConfig()
	GlobalSMHandler = &ShadowMasterHandler{}
}

func (sm *ShadowMasterHandler) SendOperation(ctx context.Context, args *pb.OperationArgs) (*pb.OperationReply, error) {
	op := &internal.Operation{}
	switch args.Type {
	case internal.Operation_Add:
		op.Type = internal.Operation_Add
		op.Des = args.Des
		op.IsFile = args.IsFile
	case internal.Operation_Move:
		op.Type = internal.Operation_Move
		op.Src = args.Src
		op.Des = args.Des
	case internal.Operation_Remove:
		op.Type = internal.Operation_Remove
		op.Des = args.Des
	case internal.Operation_Rename:
		op.Type = internal.Operation_Rename
		op.Src = args.Src
		op.Des = args.Des
	default:
		errMsg := "Invalid Operation Type"
		details, _ := status.New(codes.InvalidArgument, errMsg).WithDetails(&pb.RPCError{
			Code: common.ShadowMasterSendOperationFailed,
			Msg:  errMsg,
		})
		return &pb.OperationReply{Ok: false}, details.Err()
	}
	err := DoSendOperation(op)
	if err != nil {
		details, _ := status.New(codes.Unknown, err.Error()).WithDetails(&pb.RPCError{
			Code: common.ShadowMasterSendOperationFailed,
			Msg:  err.Error(),
		})
		return &pb.OperationReply{Ok: false}, details.Err()
	}
	return &pb.OperationReply{Ok: true}, nil
}
