package internal

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc/peer"
	"strconv"
	"time"
	"tinydfs-base/common"
	"tinydfs-base/protocol/pb"
)

const (
	IsFile4File  = true
	replicateNum = 3
)

// DoRegister 由chunkserver通过rpc调用该方法，将对应DataNode注册到本NameNode上
func DoRegister(ctx context.Context) (string, string, error) {
	var (
		id      string
		address string
	)
	p, _ := peer.FromContext(ctx)
	id = uuid.NewString()
	address = p.Addr.String()
	logrus.WithContext(ctx).Infof("Receive register request from %s", address)

	// 定时器，10秒无心跳则等待重连，十分钟无心跳则判定离线
	waitTimer := time.NewTimer(time.Duration(viper.GetInt(common.ChunkWaitTime)) * time.Second)
	dieTimer := time.NewTimer(time.Duration(viper.GetInt(common.ChunkDieTime)) * time.Second)
	datanode := &DataNode{
		Id:        id,
		status:    common.Alive,
		Address:   address,
		Chunks:    []string{},
		waitTimer: waitTimer,
		dieTimer:  dieTimer,
	}
	AddDataNode(datanode)
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
	return id, address, nil
}

// DoHeartbeat 接收来自chunkserver的心跳，重置计时器
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

func DoCheckArgs4Add(args *pb.CheckArgs4AddArgs) (string, int32, error) {
	fileNode, stack, err := LockAndAddFileNode(args.Path, args.FileName, args.Size, IsFile4File)
	fileNodesMapLock.Lock()
	unlockedFileNodes[fileNode.Id] = stack
	fileNodesMapLock.Unlock()
	if err != nil {
		return "", 0, err
	}
	return fileNode.Id, int32(len(fileNode.Chunks)), nil
}

func DoGetDataNodes4Add(fileNodeId string, chunkIndex int32) ([]string, string, error) {
	chunkId := fileNodeId + strconv.Itoa(int(chunkIndex))
	dataNodes, primaryNode := AllocateDataNodes(chunkId, replicateNum)
	chunk := &Chunk{
		Id:          chunkId,
		dataNodes:   dataNodes,
		primaryNode: primaryNode,
	}
	AddChunk(chunk)
	return dataNodes, primaryNode, nil
}
