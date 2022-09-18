package internal

import (
	"context"
	"fmt"
	set "github.com/deckarep/golang-set"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc/peer"
	"strconv"
	"strings"
	"time"
	"tinydfs-base/common"
	"tinydfs-base/protocol/pb"
)

const (
	isFile4File = true
)

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

func DoCheckArgs4Add(args *pb.CheckArgs4AddArgs) (string, int32, error) {
	fileNode, stack, err := LockAndAddFileNode(args.Path, args.FileName, args.Size, isFile4File)
	fileNodesMapLock.Lock()
	lockedFileNodes[fileNode.Id] = stack
	fileNodesMapLock.Unlock()
	if err != nil {
		return "", 0, err
	}
	return fileNode.Id, int32(len(fileNode.Chunks)), nil
}

func DoGetDataNodes4Add(fileNodeId string, chunkIndex int32) ([]string, string, error) {
	chunkId := fileNodeId + common.ChunkIdDelimiter + strconv.Itoa(int(chunkIndex))
	dataNodes, primaryNode := AllocateDataNodes()
	var (
		dataNodeIds   = make([]string, len(dataNodes))
		dataNodeAddrs = make([]string, len(dataNodes))
	)
	for i, node := range dataNodes {
		node.Chunks.Add(chunkId)
		dataNodeIds[i] = node.Id
		dataNodeAddrs[i] = node.Address
	}
	primaryNode.Leases.Add(chunkId)

	chunk := &Chunk{
		Id:          chunkId,
		dataNodes:   dataNodeIds,
		primaryNode: primaryNode.Id,
	}
	AddChunk(chunk)
	return dataNodeAddrs, primaryNode.Address, nil
}

func DoUnlockDic4Add(fileNodeId string, isRead bool) error {
	err := UnlockFileNodesById(fileNodeId, isRead)
	if err != nil {
		return err
	}

	return nil
}

func DoReleaseLease4Add(chunkId string) error {
	chunk := GetChunk(chunkId)
	err := ReleaseLease(chunk.primaryNode, chunkId)
	if err != nil {
		return err
	}
	return nil
}

func DoCheckAndMkdir(path string, dirName string) error {
	_, err := AddFileNode(path, dirName, common.DirSize, false)
	if err != nil {
		return err
	}
	return nil
}
