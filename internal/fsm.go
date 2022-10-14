package internal

import (
	"bufio"
	"encoding/json"
	"github.com/hashicorp/raft"
	"io"
	"reflect"
)

// ApplyResponse is the reply of MasterFSM's Apply function.
type ApplyResponse struct {
	Response interface{}
	Error    error
}

// MasterFSM implements FSM and make use of the replicated log.
type MasterFSM struct {
}

// Apply calls Apply function of operation, changes to metadata will be made
// in that function.
func (ms MasterFSM) Apply(l *raft.Log) interface{} {
	operation := ConvBytes2Operation(l.Data)
	response, err := operation.Apply()
	return &ApplyResponse{
		Response: response,
		Error:    err,
	}
}

// ConvBytes2Operation uses reflect to restore operation from data.
func ConvBytes2Operation(data []byte) Operation {
	opContainer := OpContainer{}
	var operation Operation
	err := json.Unmarshal(data, &opContainer)
	if err != nil {
		return nil
	}
	operation = reflect.New(OpTypeMap[opContainer.OpType]).Interface().(Operation)
	err = json.Unmarshal(opContainer.OpData, operation)
	if err != nil {
		return nil
	}
	return operation
}

func (ms MasterFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &snapshot{}, nil
}

// Restore read snapshot and restore metadata from it. There are three part of metadata
// need to be restored: directory tree, DataNode information and Chunk information
func (ms MasterFSM) Restore(r io.ReadCloser) error {
	buf := bufio.NewScanner(r)
	err := RestoreDirTree(buf)
	if err != nil {
		return err
	}
	err = RestoreDataNodes(buf)
	if err != nil {
		return err
	}
	err = RestoreChunks(buf)
	if err != nil {
		return err
	}
	return r.Close()
}

type snapshot struct {
}

// Persist Take a snapshot of current metadata and save it as a file.
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	err := PersistDirTree(sink)
	if err != nil {
		return err
	}
	err = PersistDataNodes(sink)
	if err != nil {
		return err
	}
	err = PersistChunks(sink)
	if err != nil {
		return err
	}
	return sink.Close()
}

func (s *snapshot) Release() {

}
