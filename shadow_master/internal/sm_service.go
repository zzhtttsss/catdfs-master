package internal

import (
	"tinydfs-base/protocol/pb"
)

func DoSendOperation(op *pb.OperationArgs) error {
	err := GlobalSMHandler.SM.Info(op)
	if err != nil {
		return err
	}
	return nil
}
