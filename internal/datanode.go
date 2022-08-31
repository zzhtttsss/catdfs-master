package internal

import (
	mapset "github.com/deckarep/golang-set"
	"sync"
	"time"
)

var (
	// Store all DataNode, using id as the key
	dataNodeMap    = make(map[string]*DataNode)
	updateMapLock  = &sync.RWMutex{}
	dataNodeHeap   = DataNodeHeap{}
	updateHeapLock = &sync.RWMutex{}
)

type DataNode struct {
	Id string
	// 0 died; 1 alive; 2 waiting
	status  int
	Address string
	// id of all Chunk in this file.
	Chunks    mapset.Set
	Leases    mapset.Set
	waitTimer *time.Timer
	dieTimer  *time.Timer
}

type DataNodeHeap []*DataNode

func (h DataNodeHeap) Len() int {
	return len(h)
}

func (h DataNodeHeap) Less(i, j int) bool {
	return h[i].Chunks.Cardinality() > h[j].Chunks.Cardinality()
}

func (h DataNodeHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *DataNodeHeap) Push(v interface{}) {
	*h = append(*h, v.(*DataNode))
}

func (h *DataNodeHeap) Pop() interface{} {
	last := len(*h) - 1
	v := (*h)[last]
	*h = (*h)[:last]
	return v
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

func AllocateDataNodes(id string, replicaNum int) ([]*DataNode, *DataNode) {
	updateHeapLock.Lock()
	dataNodes := make([]*DataNode, replicaNum)
	copy(dataNodes, dataNodeHeap)
	// 先选出包含chunk数量前replicaNum小的dataNode, 然后首先根据持有租约数量最少，再根据持有chunk数量最少，最后随机的方式选出primary
	updateHeapLock.Unlock()
	primaryNode := dataNodes[0]
	for _, node := range dataNodes {
		if node.Leases.Cardinality() < primaryNode.Leases.Cardinality() {
			primaryNode = node
		}
	}
	adjust4Allocate()
	return dataNodes, primaryNode
}

func RemoveChunk(chunkId, node *DataNode) {
	node.Chunks.Remove(chunkId)
	adjust4Remove(node)
}

func adjust4Allocate() {
	updateMapLock.RLock()
	dataNodeHeap = dataNodeHeap[0:0]
	for _, node := range dataNodeMap {
		adjust(node)
	}
	updateMapLock.RUnlock()

}

func adjust4Remove(node *DataNode) {
	adjust(node)
}

func adjust(node *DataNode) {
	if dataNodeHeap.Len() < replicaNum {
		dataNodeHeap.Push(node)
	} else {
		topNode := dataNodeHeap.Pop().(*DataNode)
		if topNode.Chunks.Cardinality() > node.Chunks.Cardinality() {
			dataNodeHeap.Push(node)
		} else {
			dataNodeHeap.Push(topNode)
		}
	}
}
