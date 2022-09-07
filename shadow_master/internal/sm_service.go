package internal

import "tinydfs-master/internal"

func DoSendOperation(op *internal.Operation) error {
	err := GlobalSMHandler.Info(op)
	if err != nil {
		return err
	}
	return nil
}
