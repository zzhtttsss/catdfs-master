package internal

import (
	"bufio"
	"container/heap"
	"context"
	"fmt"
	mapset "github.com/deckarep/golang-set"
	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"strconv"
	"strings"
	"sync"
	"time"
	"tinydfs-base/common"
	"tinydfs-base/util"
)

const (
	dataNodeIdIdx = iota
	statusIdx
	addressIdx
	DNChunksIdx
	leaseIdx
	heartbeatIdx
)

var (
	// Store all DataNode, using id as the key
	dataNodeMap   = make(map[string]*DataNode)
	updateMapLock = &sync.RWMutex{}
	//a heap with capacity "ReplicaNum". It is used to store the first "ReplicaNum" dataNodes
	// with the least number of memory blocks.
	dataNodeHeap = DataNodeHeap{
		dns:  make([]*DataNode, 0),
		less: &MaxHeapFunc{},
	}
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
	Leases        mapset.Set
	HeartbeatTime time.Time
}

func (d *DataNode) String() string {
	res := strings.Builder{}
	chunks := make([]string, d.Chunks.Cardinality())
	chunkChan := d.Chunks.Iter()
	index := 0
	for chunkId := range chunkChan {
		chunks[index] = chunkId.(string)
		index++
	}

	leases := make([]string, d.Leases.Cardinality())
	leaseChan := d.Leases.Iter()
	index = 0
	for chunkId := range leaseChan {
		leases[index] = chunkId.(string)
		index++
	}

	res.WriteString(fmt.Sprintf("%s$%v$%s$%v$%v$%s\n",
		d.Id, d.status, d.Address, chunks, leases, d.HeartbeatTime.Format(common.LogFileTimeFormat)))
	return res.String()
}

// MonitorHeartbeat Run in a goroutine.
// This function will monitor heartbeat of all DataNode. It will scan dataNodeMap every once in a while and change the
// status of DataNode which with no heartbeat for ten minutes.
func MonitorHeartbeat(ctx context.Context) {
	for {
		select {
		default:
			updateMapLock.Lock()
			for _, node := range dataNodeMap {
				// give died datanode a second chance to restart.
				if int(time.Now().Sub(node.HeartbeatTime).Seconds()) > viper.GetInt(common.ChunkDieTime) {
					node.status = common.Died
					continue
				}
				// if node is still died, master will move all chunks belonging to this node.
				if node.status == common.Died {
					csCountMonitor.Dec()
					GlobalMasterHandler.Shrink(node)
					operation := &DeregisterOperation{
						Id:         util.GenerateUUIDString(),
						DataNodeId: node.Id,
					}
					data := getData4Apply(operation, common.OperationDeregister)
					_ = GlobalMasterHandler.Raft.Apply(data, 5*time.Second)
				}
			}
			updateMapLock.Unlock()
			logrus.WithContext(ctx).Infof("Complete a round of check, time: %s", time.Now().String())
			time.Sleep(time.Duration(viper.GetInt(common.MasterCheckTime)) * time.Second)
		case <-ctx.Done():
			return
		}
	}
}

// DataNodeHeap is a heap using strategy pattern to specify whether it is a max heap or a min heap.
type DataNodeHeap struct {
	dns  []*DataNode
	less LessStrategy
}

type LessStrategy interface {
	LessFunc(h []*DataNode, i int, j int) bool
}

type MaxHeapFunc struct{}

func (m *MaxHeapFunc) LessFunc(h []*DataNode, i int, j int) bool {
	return h[i].Chunks.Cardinality() > h[j].Chunks.Cardinality()
}

func (h DataNodeHeap) Len() int {
	return len(h.dns)
}

func (h DataNodeHeap) Less(i, j int) bool {
	return h.less.LessFunc(h.dns, i, j)
}

func (h DataNodeHeap) Swap(i, j int) {
	h.dns[i], h.dns[j] = h.dns[j], h.dns[i]
}

func (h *DataNodeHeap) Push(v interface{}) {
	h.dns = append(h.dns, v.(*DataNode))
}

func (h *DataNodeHeap) Pop() interface{} {
	last := len(h.dns) - 1
	v := h.dns[last]
	h.dns = h.dns[:last]
	return v
}

func AddDataNode(datanode *DataNode) {
	updateMapLock.Lock()
	defer updateMapLock.Unlock()
	dataNodeMap[datanode.Id] = datanode
}

func GetDataNode(id string) *DataNode {
	updateMapLock.RLock()
	defer updateMapLock.RUnlock()
	return dataNodeMap[id]
}

func RemoveDataNode(dataNodeId string) {
	updateMapLock.Lock()
	defer updateMapLock.Unlock()
	delete(dataNodeMap, dataNodeId)
}

// AllocateDataNodes Select several DataNode to store a Chunk. DataNode allocation strategy is:
// 1. First select the first "ReplicaNum" dataNodes with the least number of memory blocks.
// 2. Select the node with the least number of leases from these nodes as the primary DataNode of the Chunk
func AllocateDataNodes() ([]*DataNode, *DataNode) {
	updateHeapLock.Lock()
	allDataNodes := make([]*DataNode, dataNodeHeap.Len())
	copy(allDataNodes, dataNodeHeap.dns)
	updateHeapLock.Unlock()
	// TODO if there is no chunkserver in system, it will cause a panic.
	primaryNode := allDataNodes[0]
	for _, node := range allDataNodes {
		if node.Leases.Cardinality() < primaryNode.Leases.Cardinality() {
			primaryNode = node
		}
	}
	dataNodes := make([]*DataNode, len(allDataNodes)-1)
	index := 0
	for _, node := range allDataNodes {
		if node.Id != primaryNode.Id {
			dataNodes[index] = node
			index++
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
	updateHeapLock.Lock()
	dataNodeHeap.dns = dataNodeHeap.dns[0:0]
	for _, node := range dataNodeMap {
		adjust(node)
	}
	updateHeapLock.Unlock()
	updateMapLock.RUnlock()

}

func adjust4Remove(node *DataNode) {
	updateHeapLock.Lock()
	adjust(node)
	updateHeapLock.Unlock()
}

func Adjust4Add(node *DataNode) {
	updateHeapLock.Lock()
	adjust(node)
	updateHeapLock.Unlock()
}

func adjust(node *DataNode) {
	if dataNodeHeap.Len() < viper.GetInt(common.ReplicaNum) {
		heap.Push(&dataNodeHeap, node)
	} else {
		topNode := heap.Pop(&dataNodeHeap).(*DataNode)
		if topNode.Chunks.Cardinality() > node.Chunks.Cardinality() {
			heap.Push(&dataNodeHeap, node)
		} else {
			heap.Push(&dataNodeHeap, topNode)
		}
	}
}

func ReleaseLease(dataNodeId string, chunkId string) error {
	updateMapLock.RLock()
	defer updateMapLock.RUnlock()
	dataNode, ok := dataNodeMap[dataNodeId]
	if !ok {
		return fmt.Errorf("fail to get FileNode from dataNodeMap, fileNodeId : %s", dataNodeId)
	}
	dataNode.Leases.Remove(chunkId)
	return nil
}

func PersistDataNodes(sink raft.SnapshotSink) error {
	for _, dataNode := range dataNodeMap {
		_, err := sink.Write([]byte(dataNode.String()))
		if err != nil {
			return err
		}
	}
	_, err := sink.Write([]byte(common.SnapshotDelimiter))
	if err != nil {
		return err
	}
	return nil
}

func RestoreDataNodes(buf *bufio.Scanner) error {
	var (
		chunks = mapset.NewSet()
		leases = mapset.NewSet()
	)

	for buf.Scan() {
		line := buf.Text()
		if line == common.SnapshotDelimiter {
			return nil
		}
		data := strings.Split(line, "$")

		chunksLen := len(data[DNChunksIdx])
		chunksData := data[DNChunksIdx][1 : chunksLen-1]
		for _, chunkId := range strings.Split(chunksData, " ") {
			chunks.Add(chunkId)
		}
		leasesLen := len(data[leaseIdx])
		leasesData := data[leaseIdx][1 : leasesLen-1]
		for _, chunkId := range strings.Split(leasesData, " ") {
			leases.Add(chunkId)
		}
		heartbeatTime, _ := time.Parse(common.LogFileTimeFormat, data[heartbeatIdx])
		status, _ := strconv.Atoi(data[statusIdx])
		dataNodeMap[data[dataNodeIdIdx]] = &DataNode{
			Id:            data[dataNodeIdIdx],
			status:        status,
			Address:       data[addressIdx],
			Chunks:        chunks,
			Leases:        leases,
			HeartbeatTime: heartbeatTime,
		}
	}
	return nil
}
