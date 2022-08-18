package model

import (
	"context"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/peer"
	"sync"
	"time"
	"tinydfs-base/common"
)

type NameNode struct {
	DataNodeMap map[string]*DataNode
	Namespace   *Namespace
	mu          *sync.Mutex
}

//CreateNameNode 创建NameNode
func CreateNameNode() *NameNode {
	return &NameNode{
		DataNodeMap: make(map[string]*DataNode),
		Namespace:   CreateNamespace(),
		mu:          &sync.Mutex{},
	}
}

type DataNode struct {
	Id        string
	status    int // 0 died ; 1 status ; 2 waiting
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

	// 定时器，10秒无心跳则等待重连，十分钟无心跳则判定离线
	waitTimer := time.NewTimer(10 * time.Second)
	dieTimer := time.NewTimer(1 * time.Minute)
	nn.mu.Lock()
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
			logrus.WithContext(ctx).Infof("ID: %s is waiting reconnect", id)
			waitTimer.Stop()
			dieTimer.Reset(1 * time.Minute)
		}
	}(ctx)
	go func(ctx context.Context) {
		<-dieTimer.C
		nn.DataNodeMap[id].status = common.Died
		dieTimer.Stop()
		logrus.WithContext(ctx).Infof("ID: %s is died", id)
	}(ctx)

	logrus.WithContext(ctx).Infof("ID: %s is connected", id)
	return id, address, nil
}
