package internal

import (
	"sync"
	"time"
)

var (
	dataNodeMap   = make(map[string]*DataNode)
	updateMapLock = sync.RWMutex{}
)

type DataNode struct {
	Id        string
	status    int // 0 died ; 1 alive ; 2 waiting
	Address   string
	Chunks    []string
	waitTimer *time.Timer
	dieTimer  *time.Timer
}

func AddDataNode(datanode *DataNode) {
	updateMapLock.Lock()
	dataNodeMap[datanode.Id] = datanode
	updateMapLock.Unlock()
}

func GetDataNode(id string) *DataNode {
	updateMapLock.RLock()
	defer func() {
		updateMapLock.RUnlock()
	}()
	return dataNodeMap[id]
}

func AllocateDataNodes(id string, replicateNum int) ([]string, string) {
	updateMapLock.RLock()
	// 先选出包含chunk数量前replicateNum小的dataNode, 然后首先根据持有租约数量最少，再根据持有chunk数量最少，最后随机的方式选出primary
	defer func() {
		updateMapLock.RUnlock()
	}()
	return nil, ""
}
