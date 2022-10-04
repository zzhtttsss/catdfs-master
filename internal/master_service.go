package internal

import (
	"container/list"
	"context"
	"encoding/json"
	"fmt"
	set "github.com/deckarep/golang-set"
	"github.com/google/uuid"
	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc/peer"
	"io"
	"reflect"
	"strings"
	"time"
	"tinydfs-base/common"
)

const (
	isFile4File = true
)

type ApplyResponse struct {
	Response interface{}
	Error    error
}

type MasterFSM struct {
}

// Apply This function will call Apply function of operation, changes to metadata will be made in that function
func (ms MasterFSM) Apply(l *raft.Log) interface{} {
	operation := ConvBytes2Operation(l.Data)
	response, err := operation.Apply()
	return ApplyResponse{
		Response: response,
		Error:    err,
	}
}

// ConvBytes2Operation Use reflect to restore operation from data
func ConvBytes2Operation(data []byte) Operation {
	opContainer := OpContainer{}
	var operation Operation
	err := json.Unmarshal(data, &opContainer)
	if err != nil {
		return nil
	}
	operation = reflect.New(OperationTypes[opContainer.OpType]).Interface().(Operation)
	err = json.Unmarshal(opContainer.OpData, operation)
	if err != nil {
		return nil
	}
	return operation
}

func (ms MasterFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &snapshot{}, nil
}

func (ms MasterFSM) Restore(r io.ReadCloser) error {
	logrus.Println("Start to restore metadata.")
	rootMap := ReadSnapshotLines(r)
	if rootMap != nil && len(rootMap) != 0 {
		logrus.Println("Start to Deserialize directory tree.")
		root = RootDeserialize(rootMap)
	}
	return r.Close()
}

type snapshot struct {
}

// Persist Take a snapshot of current metadata.
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	queue := list.New()
	queue.PushBack(root)
	for queue.Len() != 0 {
		cur := queue.Front()
		queue.Remove(cur)
		node, ok := cur.Value.(*FileNode)
		if !ok {
			logrus.Warnf("Fail to convert element to FileNode")
		}
		_, err := sink.Write([]byte(node.String()))
		if err != nil {
			logrus.Errorf("Fail to write string, error detail %s", err.Error())
		}
		for _, child := range node.ChildNodes {
			queue.PushBack(child)
		}
	}
	return sink.Close()
}

func (s *snapshot) Release() {

}

func DoRegister(ctx context.Context) (string, error) {
	var (
		id      string
		address string
	)
	p, _ := peer.FromContext(ctx)
	id = uuid.NewString()
	address = strings.Split(p.Addr.String(), ":")[0]

	logrus.WithContext(ctx).Infof("Receive register request from %s", address)

	// 定时器，10秒无心跳则等待重连，十分钟无心跳则判定离线
	waitTimer := time.NewTimer(time.Duration(viper.GetInt(common.ChunkWaitTime)) * time.Second)
	dieTimer := time.NewTimer(time.Duration(viper.GetInt(common.ChunkDieTime)) * time.Second)
	datanode := &DataNode{
		Id:        id,
		status:    common.Alive,
		Address:   address,
		Chunks:    set.NewSet(),
		Leases:    set.NewSet(),
		waitTimer: waitTimer,
		dieTimer:  dieTimer,
	}
	AddDataNode(datanode)
	Adjust4Add(datanode)
	dieTimer.Stop()
	go func(ctx context.Context) {
		for {
			<-waitTimer.C
			datanode.status = common.Waiting
			logrus.WithContext(ctx).Infof("[Id=%s] Waiting reconnect", id)
			waitTimer.Stop()
			dieTimer.Reset(1 * time.Minute)
		}
	}(ctx)
	go func(ctx context.Context) {
		<-dieTimer.C
		datanode.status = common.Died
		dieTimer.Stop()
		logrus.WithContext(ctx).Infof("[Id=%s] Died", id)
	}(ctx)

	logrus.WithContext(ctx).Infof("[Id=%s] Connected", id)
	return id, nil
}

func DoHeartbeat(Id string) error {
	if dataNode := GetDataNode(Id); dataNode != nil {
		dataNode.waitTimer.Stop()
		dataNode.dieTimer.Stop()
		dataNode.waitTimer.Reset(10 * time.Second)
		if dataNode.status == common.Waiting {
			dataNode.status = common.Alive
		}
		return nil
	}
	return fmt.Errorf("datanode %s not exist", Id)
}
