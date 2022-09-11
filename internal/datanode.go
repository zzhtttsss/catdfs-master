package internal

import (
	mapset "github.com/deckarep/golang-set"
	"github.com/spf13/viper"
	"sync"
	"time"
	"tinydfs-base/common"
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
	// id of all Chunk stored in this DataNode.
	Chunks mapset.Set
	// id of all Chunk that primary DataNode is this DataNode.
	Leases    mapset.Set
	waitTimer *time.Timer
	dieTimer  *time.Timer
}

// DataNodeHeap Max heap with capacity "ReplicaNum". It is used to store the first "ReplicaNum" dataNodes
// with the least number of memory blocks
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

// AllocateDataNodes Select several DataNode to store a Chunk. DataNode allocation strategy is:
// 1. First select the first "ReplicaNum" dataNodes with the least number of memory blocks.
// 2. Select the node with the least number of leases from these nodes as the primary DataNode of the Chunk
func AllocateDataNodes() ([]*DataNode, *DataNode) {
	updateHeapLock.Lock()
	allDataNodes := make([]*DataNode, viper.GetInt(common.ReplicaNum))
	copy(allDataNodes, dataNodeHeap)
	updateHeapLock.Unlock()
	primaryNode := allDataNodes[0]
	for _, node := range allDataNodes {
		if node.Leases.Cardinality() < primaryNode.Leases.Cardinality() {
			primaryNode = node
		}
	}
	dataNodes := make([]*DataNode, viper.GetInt(common.ReplicaNum)-1)
	for _, node := range allDataNodes {
		if node.Id != primaryNode.Id {
			dataNodes = append(dataNodes, node)
		}
	}
	adjust4Allocate()
	return dataNodes, primaryNode
}

func RemoveChunk(chunkId string, node *DataNode) {
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
	if dataNodeHeap.Len() < viper.GetInt(common.ReplicaNum) {
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
