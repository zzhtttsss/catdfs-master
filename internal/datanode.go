package internal

import (
	"bufio"
	"container/heap"
	"context"
	"fmt"
	set "github.com/deckarep/golang-set"
	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"tinydfs-base/common"
	"tinydfs-base/protocol/pb"
	"tinydfs-base/util"
)

const (
	dataNodeIdIdx = iota
	statusIdx
	addressIdx
	dnChunksIdx
	ioLoadIdx
	heartbeatIdx
)

var (
	// dataNodeMap stores all DataNode in this system, using id as the key.
	dataNodeMap   = make(map[string]*DataNode)
	updateMapLock = &sync.RWMutex{}
	// dataNodeHeap is a max heap with capacity "ReplicaNum". It is used to store
	// the first "ReplicaNum" dataNodes with the least number of memory blocks.
	// This heap will not actively keep the latest status. So if you want to get
	// the latest dataNodeHeap, you must call AllocateDataNodes to update dataNodeHeap
	// first.
	dataNodeHeap = DataNodeHeap{
		dns:  make([]*DataNode, 0),
		less: &MaxHeapFunc{},
	}
	updateHeapLock = &sync.RWMutex{}
)

// DataNode represents a chunkserver in the file system.
type DataNode struct {
	Id string
	// status 0 died; 1 alive
	status  int
	Address string
	// Chunks includes all Chunk's id stored in this DataNode.
	Chunks set.Set
	// Deprecated: Leases includes Chunk's id that primary DataNode is this DataNode.
	Leases set.Set
	// IOLoad represents IO load of a DataNode. It is flushed by DataNode's heartbeat,
	// so it will have a delay of a few seconds.
	IOLoad int
	// FutureSendChunks include ChunkSendInfo that means this DataNode should send
	// which Chunk to Which DataNode, and the value represent the state of sending.
	FutureSendChunks map[ChunkSendInfo]int
	// HeartbeatTime is the time when the most recent heartbeat was received for
	// this node.
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

	res.WriteString(fmt.Sprintf("%s$%v$%s$%v$%v$%s\n",
		d.Id, d.status, d.Address, chunks, d.IOLoad, d.HeartbeatTime.Format(common.LogFileTimeFormat)))
	return res.String()
}

// MonitorHeartbeat runs in a goroutine. This function monitor heartbeat of
// all DataNode. It will check all DataNode in dataNodeMap every 1 minute,
// there are 3 situations:
// 1. We have received heartbeat of this DataNode in 30 seconds. if the status
//    of it is waiting, we will set status to alive, or we will do nothing.
// 2. The status of DataNode is alive, and we have not received heartbeat of it
//    over 30 seconds, we will set status to waiting.
// 3. The status of DataNode is waiting, and we have not received heartbeat of it
//    over 10 minute, we will think this DataNode is dead and start a shrink.
func MonitorHeartbeat(ctx context.Context) {
	for {
		select {
		default:
			updateMapLock.Lock()
			for _, node := range dataNodeMap {
				logrus.Debugf("Datanode id: %s, chunk set: %s", node.Id, node.Chunks.String())
				// Give died datanode a second chance to restart.
				if int(time.Now().Sub(node.HeartbeatTime).Seconds()) > viper.GetInt(common.ChunkWaitingTime)*
					viper.GetInt(common.ChunkHeartbeatTime) && node.status == common.Alive {
					operation := &DegradeOperation{
						Id:         util.GenerateUUIDString(),
						DataNodeId: node.Id,
						Stage:      common.Degrade2Waiting,
					}
					data := getData4Apply(operation, common.OperationDegrade)
					_ = GlobalMasterHandler.Raft.Apply(data, 5*time.Second)
					continue
				}
				if int(time.Now().Sub(node.HeartbeatTime).Seconds()) > viper.GetInt(common.ChunkDieTime) &&
					node.status == common.Waiting {
					csCountMonitor.Dec()
					operation := &DegradeOperation{
						Id:         util.GenerateUUIDString(),
						DataNodeId: node.Id,
						Stage:      common.Degrade2Dead,
					}
					data := getData4Apply(operation, common.OperationDegrade)
					_ = GlobalMasterHandler.Raft.Apply(data, 5*time.Second)
					continue
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

// DataNodeHeap is max heap with capacity "ReplicaNum". It is used to store the
// first "ReplicaNum" DataNode with the least number of memory blocks.
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

// UpdateDataNode4Heartbeat updates DataNode according to the Chunk sending
// information given by the heartbeat.
func UpdateDataNode4Heartbeat(o HeartbeatOperation) ([]ChunkSendInfo, bool) {
	updateMapLock.Lock()
	defer updateMapLock.Unlock()
	dataNode, ok := dataNodeMap[o.DataNodeId]
	if !ok {
		return nil, false
	}
	dataNode.HeartbeatTime = time.Now()
	if o.IsReady {
		dataNode.status = common.Alive
	}
	dataNode.IOLoad = int(o.IOLoad)
	for _, info := range o.SuccessInfos {
		delete(dataNode.FutureSendChunks, info)
		dataNodeMap[info.DataNodeId].Chunks.Add(info.ChunkId)
	}
	for _, info := range o.FailInfos {
		delete(dataNode.FutureSendChunks, info)
		pendingChunkQueue.Push(String(info.ChunkId))
	}
	nextChunkInfos := make([]ChunkSendInfo, 0, len(dataNode.FutureSendChunks))
	for info, i := range dataNode.FutureSendChunks {
		if i != common.WaitToSend {
			nextChunkInfos = append(nextChunkInfos, info)
			dataNode.FutureSendChunks[info] = common.WaitToSend
		}
	}
	return nextChunkInfos, true
}

func GetSortedDataNodeIds(set set.Set) ([]string, []string) {
	updateMapLock.RLock()
	defer updateMapLock.RUnlock()
	setChan := set.Iter()

	dns := make([]*DataNode, 0)
	for id := range setChan {
		if node, ok := dataNodeMap[id.(string)]; ok {
			if node.status == common.Alive {
				dns = append(dns, dataNodeMap[id.(string)])
			}
		}
	}
	sort.SliceStable(dns, func(i, j int) bool {
		if dns[i].IOLoad < dns[j].IOLoad {
			return true
		}
		return false
	})
	ids := make([]string, len(dns))
	adds := make([]string, len(dns))
	for i, dn := range dns {
		ids[i] = dn.Id
		adds[i] = dn.Address
	}
	return ids, adds
}

func GetAliveDataNodeIds() []string {
	updateMapLock.RLock()
	defer updateMapLock.RUnlock()
	ids := make([]string, 0, len(dataNodeMap))
	for id, node := range dataNodeMap {
		if node.status == common.Alive {
			ids = append(ids, id)
		}
	}
	return ids
}

func GetDataNodeAddresses(chunkSendInfos []ChunkSendInfo) []string {
	updateMapLock.RLock()
	defer updateMapLock.RUnlock()
	adds := make([]string, 0, len(dataNodeMap))
	for _, info := range chunkSendInfos {
		adds = append(adds, dataNodeMap[info.DataNodeId].Address)
	}
	return adds
}

// BatchApplyPlan2DataNode use the given plan to allocate Chunk for each DataNode.
func BatchApplyPlan2DataNode(receiverPlan []int, senderPlan []int, chunkIds []string, dataNodeIds []string) {
	updateMapLock.Lock()
	defer updateMapLock.Unlock()
	for i, dnIndex := range senderPlan {
		chunkSendInfo := ChunkSendInfo{
			ChunkId:    chunkIds[i],
			DataNodeId: dataNodeIds[receiverPlan[i]],
			SendType:   common.CopySendType,
		}
		dataNodeMap[dataNodeIds[dnIndex]].FutureSendChunks[chunkSendInfo] = common.WaitToInform
	}
}

func BatchAddChunks(infos []util.ChunkSendResult) {
	updateMapLock.Lock()
	defer updateMapLock.Unlock()
	for _, info := range infos {
		for _, id := range info.SuccessDataNodes {
			if dataNode, ok := dataNodeMap[id]; ok {
				dataNode.Chunks.Add(info.ChunkId)
			}
		}
	}
}

type ChunkSendInfo struct {
	ChunkId    string `json:"chunk_id"`
	DataNodeId string `json:"data_node_id"`
	SendType   int    `json:"send_type"`
}

func ConvChunkInfo(chunkInfos []*pb.ChunkInfo) []ChunkSendInfo {
	chunkSendInfos := make([]ChunkSendInfo, len(chunkInfos))
	for i := 0; i < len(chunkInfos); i++ {
		chunkSendInfos[i] = ChunkSendInfo{
			ChunkId:    chunkInfos[i].ChunkId,
			DataNodeId: chunkInfos[i].DataNodeId,
			SendType:   int(chunkInfos[i].SendType),
		}
	}
	return chunkSendInfos
}

func DeConvChunkInfo(chunkSendInfos []ChunkSendInfo) []*pb.ChunkInfo {
	chunkInfos := make([]*pb.ChunkInfo, len(chunkSendInfos))
	for i := 0; i < len(chunkInfos); i++ {
		chunkInfos[i] = &pb.ChunkInfo{
			ChunkId:    chunkSendInfos[i].ChunkId,
			DataNodeId: chunkSendInfos[i].DataNodeId,
			SendType:   int32(chunkSendInfos[i].SendType),
		}
	}
	return chunkInfos
}

// DegradeDataNode degrade a DataNode based on given stage. If DataNode is dead,
// it will remove DataNode from dataNodeMap and put all Chunk's id in Chunks and
// FutureSendChunks of the DataNode to pendingChunkQueue so that system can make
// up the missing copies later
func DegradeDataNode(dataNodeId string, stage int) {
	logrus.Infof("Start to degrade, datanode id: %s, stage: %v", dataNodeId, stage)
	updateMapLock.Lock()
	defer updateMapLock.Unlock()
	dataNode, ok := dataNodeMap[dataNodeId]
	if !ok {
		return
	}
	if stage == common.Degrade2Waiting {
		dataNode.status = common.Waiting
		return
	}
	delete(dataNodeMap, dataNodeId)
	logrus.Debugf("Degrade datanode chunks is: %s, len is: %v", dataNode.Chunks.String(), dataNode.Chunks.Cardinality())
	for _, chunkId := range dataNode.Chunks.ToSlice() {
		pendingChunkQueue.Push(String(chunkId.(string)))
	}
	BatchClearDataNode(dataNode.Chunks.ToSlice(), dataNodeId)
	for info := range dataNode.FutureSendChunks {
		pendingChunkQueue.Push(String(info.ChunkId))
	}
	logrus.Infof("Success to degrade, datanode id: %s, stage: %v", dataNodeId, stage)
}

// AllocateDataNodes Select several DataNode to store a Chunk. DataNode allocation
// strategy is:
// 1. Reload dataNodeHeap with all DataNode.
// 2. Select the first "ReplicaNum" dataNodes with the least number of memory Chunk.
func AllocateDataNodes() []*DataNode {
	updateMapLock.RLock()
	updateHeapLock.Lock()
	dataNodeHeap.dns = dataNodeHeap.dns[0:0]
	for _, node := range dataNodeMap {
		if node.status == common.Alive {
			adjust(node)
		}
	}
	// Todo if Chunk num is same, choose the DataNode with less IOLoad.
	allDataNodes := make([]*DataNode, dataNodeHeap.Len())
	copy(allDataNodes, dataNodeHeap.dns)
	updateHeapLock.Unlock()
	updateMapLock.RUnlock()
	return allDataNodes
}

// BatchAllocateDataNodes allocate DataNode for a batch of Chunk. Each Chunk will
// get ReplicaNum DataNode to store it.
func BatchAllocateDataNodes(chunkNum int) [][]*DataNode {
	updateMapLock.RLock()
	updateHeapLock.Lock()
	processMap := make(map[*DataNode]int)
	allDataNodes := make([][]*DataNode, chunkNum)
	for _, node := range dataNodeMap {
		if node.status == common.Alive {
			processMap[node] = 0
		}
	}
	for i := 0; i < chunkNum; i++ {
		// Todo if Chunk num is same, choose the DataNode with less IOLoad.
		dataNodeHeap.dns = dataNodeHeap.dns[0:0]
		for _, node := range dataNodeMap {
			if node.status == common.Alive {
				adjust4batch(node, processMap)
			}
		}
		currentDataNodes := make([]*DataNode, dataNodeHeap.Len())
		copy(currentDataNodes, dataNodeHeap.dns)
		for _, node := range currentDataNodes {
			processMap[node] += 1
		}
		allDataNodes[i] = currentDataNodes
	}
	updateHeapLock.Unlock()
	updateMapLock.RUnlock()
	return allDataNodes
}

// adjust tries to put a DataNode into dataNodeHeap. If this DataNode meets the
// requirements of dataNodeHeap, put it into dataNodeHeap, otherwise do nothing.
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

// adjust4batch is used when provisional results(have not been flush to Chunks of
// DataNode) need to be considered. It tries to put a DataNode into dataNodeHeap
// considering the processMap given. The processMap contains how many Chunk have
// been allocated to those alive DataNode until now. If this DataNode meets the
// requirements of dataNodeHeap, put it into dataNodeHeap, otherwise do nothing.
func adjust4batch(node *DataNode, processMap map[*DataNode]int) {
	if dataNodeHeap.Len() < viper.GetInt(common.ReplicaNum) {
		heap.Push(&dataNodeHeap, node)
	} else {
		topNode := heap.Pop(&dataNodeHeap).(*DataNode)
		if topNode.Chunks.Cardinality()+processMap[topNode] > node.Chunks.Cardinality()+processMap[node] {
			heap.Push(&dataNodeHeap, node)
		} else {
			heap.Push(&dataNodeHeap, topNode)
		}
	}
}

// PersistDataNodes writes all DataNode in dataNodeMap to the sink for persistence.
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

// RestoreDataNodes reads all DataNode from the buf and puts them into dataNodeMap.
func RestoreDataNodes(buf *bufio.Scanner) error {
	var (
		chunks = set.NewSet()
	)

	for buf.Scan() {
		line := buf.Text()
		if line == common.SnapshotDelimiter {
			return nil
		}
		data := strings.Split(line, "$")

		chunksLen := len(data[dnChunksIdx])
		chunksData := data[dnChunksIdx][1 : chunksLen-1]
		for _, chunkId := range strings.Split(chunksData, " ") {
			chunks.Add(chunkId)
		}
		heartbeatTime, _ := time.Parse(common.LogFileTimeFormat, data[heartbeatIdx])
		status, _ := strconv.Atoi(data[statusIdx])
		ioLoad, _ := strconv.Atoi(data[ioLoadIdx])
		dataNodeMap[data[dataNodeIdIdx]] = &DataNode{
			Id:            data[dataNodeIdIdx],
			status:        status,
			Address:       data[addressIdx],
			Chunks:        chunks,
			IOLoad:        ioLoad,
			HeartbeatTime: heartbeatTime,
		}
	}
	return nil
}

// IsNeed2Expand finds out whether to expand.
func IsNeed2Expand(newChunkNum int) bool {
	avgChunkNum := GetAvgChunkNum()
	diff := avgChunkNum - newChunkNum
	// TODO 磁盘占有率
	if diff >= 0 || diff <= 1 {
		return false
	}
	return true
}

func GetAvgChunkNum() int {
	updateMapLock.RLock()
	defer updateMapLock.RUnlock()
	if len(dataNodeMap) == 0 {
		return 0
	}
	count := 0
	for _, node := range dataNodeMap {
		count += node.Chunks.Cardinality()
	}
	avgChunkNum := count / len(dataNodeMap)
	return avgChunkNum
}

// DoExpand gets the chunk copied according to this new dataNode.
func DoExpand(dataNode *DataNode) int {
	var (
		pendingCount  = GetAvgChunkNum()
		selfChunks    = dataNode.Chunks
		pendingChunks = set.NewSet()
		pendingMap    = map[string][]string{}
	)
For:
	for {
		notFound := true
		for _, node := range dataNodeMap {
			if node.status == common.Alive {
				for chunk := range node.Chunks.Iter() {
					if !pendingChunks.Contains(chunk) && !selfChunks.Contains(chunk) {
						notFound = false
						pendingChunks.Add(chunk)
						if pendingDataNodeChunks, ok := pendingMap[node.Id]; ok {
							pendingDataNodeChunks = append(pendingDataNodeChunks, chunk.(string))
						} else {
							pendingMap[node.Id] = []string{chunk.(string)}
						}
						if pendingChunks.Cardinality() == pendingCount {
							break For
						}
						break
					}
				}
			}
		}
		if notFound {
			break
		}
	}
	expandOperation := &ExpandOperation{
		Id:           util.GenerateUUIDString(),
		SenderPlan:   pendingMap,
		ReceiverPlan: dataNode.Id,
		ChunkIds:     util.Interfaces2TypeArr[string](pendingChunks.ToSlice()),
	}
	data := getData4Apply(expandOperation, common.OperationExpand)
	GlobalMasterHandler.Raft.Apply(data, 5*time.Second)
	return pendingChunks.Cardinality()
}
