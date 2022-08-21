package model

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"google.golang.org/grpc/peer"
	"sync"
	"time"
	"tinydfs-base/common"
)

type NameNode struct {
	DataNodeMap     map[string]*DataNode
	GlobalNamespace *Namespace
	mu              *sync.Mutex
}

//CreateNameNode 创建NameNode
func CreateNameNode() *NameNode {
	return &NameNode{
		DataNodeMap:     make(map[string]*DataNode),
		GlobalNamespace: CreateNamespace(),
		mu:              &sync.Mutex{},
	}
}

type DataNode struct {
	Id        string
	status    int // 0 died ; 1 alive ; 2 waiting
	waitTimer *time.Timer
	dieTimer  *time.Timer
}

// Register 由chunkserver通过rpc调用该方法，将对应DataNode注册到本NameNode上
func (nn *NameNode) Register(ctx context.Context) (string, string, error) {
	var (
		id      string
		address string
	)
	p, _ := peer.FromContext(ctx)
	id = uuid.NewString()
	address = p.Addr.String()
	logrus.WithContext(ctx).Info("Receive register request from %s", address)

	// 定时器，10秒无心跳则等待重连，十分钟无心跳则判定离线
	nn.mu.Lock()
	waitTimer := time.NewTimer(time.Duration(viper.GetInt(common.ChunkWaitTime)) * time.Second)
	dieTimer := time.NewTimer(time.Duration(viper.GetInt(common.ChunkDieTime)) * time.Second)
	nn.DataNodeMap[id] = &DataNode{
		Id:        id,
		status:    common.Alive,
		waitTimer: waitTimer,
		dieTimer:  dieTimer,
	}
	nn.mu.Unlock()
	dieTimer.Stop()
	go func(ctx context.Context) {
		for {
			<-waitTimer.C
			nn.DataNodeMap[id].status = common.Waiting
			logrus.WithContext(ctx).Infof("[Id=%s] Waiting reconnect", id)
			waitTimer.Stop()
			dieTimer.Reset(1 * time.Minute)
		}
	}(ctx)
	go func(ctx context.Context) {
		<-dieTimer.C
		nn.DataNodeMap[id].status = common.Died
		dieTimer.Stop()
		logrus.WithContext(ctx).Infof("[Id=%s] Died", id)
	}(ctx)

	logrus.WithContext(ctx).Infof("[Id=%s] Connected", id)
	return id, address, nil
}

// Heartbeat 接收来自chunkserver的心跳，重置计时器
func (nn *NameNode) Heartbeat(Id string) error {
	if dn, ok := nn.DataNodeMap[Id]; ok {
		dn.waitTimer.Stop()
		dn.dieTimer.Stop()
		dn.waitTimer.Reset(10 * time.Second)
		if dn.status == common.Waiting {
			dn.status = common.Alive
		}
		return nil
	}
	return fmt.Errorf("datanode %s not exist", Id)
}
