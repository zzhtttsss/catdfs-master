package internal

import (
	"encoding/json"
	"fmt"
	set "github.com/deckarep/golang-set"
	"github.com/sirupsen/logrus"
	"reflect"
	"strconv"
	"time"
	"tinydfs-base/common"
	"tinydfs-base/protocol/pb"
	"tinydfs-base/util"
)

var (
	// OpTypeMap is used to include all types of Operation. When implementing
	// a new type of Operation, we should put <name of operation, type of
	// operation> into this map.
	OpTypeMap = make(map[string]reflect.Type)
)

func init() {
	OpTypeMap[common.OperationRegister] = reflect.TypeOf(RegisterOperation{})
	OpTypeMap[common.OperationHeartbeat] = reflect.TypeOf(HeartbeatOperation{})
	OpTypeMap[common.OperationAdd] = reflect.TypeOf(AddOperation{})
	OpTypeMap[common.OperationGet] = reflect.TypeOf(GetOperation{})
	OpTypeMap[common.OperationMkdir] = reflect.TypeOf(MkdirOperation{})
	OpTypeMap[common.OperationMove] = reflect.TypeOf(MoveOperation{})
	OpTypeMap[common.OperationRemove] = reflect.TypeOf(RemoveOperation{})
	OpTypeMap[common.OperationList] = reflect.TypeOf(ListOperation{})
	OpTypeMap[common.OperationStat] = reflect.TypeOf(StatOperation{})
	OpTypeMap[common.OperationRename] = reflect.TypeOf(RenameOperation{})
	OpTypeMap[common.OperationAllocateChunks] = reflect.TypeOf(AllocateChunksOperation{})
	OpTypeMap[common.OperationExpand] = reflect.TypeOf(ExpandOperation{})
	OpTypeMap[common.OperationDegrade] = reflect.TypeOf(DegradeOperation{})
}

// Operation represents requests to make changes to metadata. If we want to modify the metadata,
// we must implement this interface and put the modification process in the Apply method.
type Operation interface {
	// Apply will perform modifications to the metadata. Calling it in the MasterFSM can ensure
	// the consistency of the metadata modification of the master cluster.
	Apply() (interface{}, error)
}

// OpContainer is used to encapsulate Operation so that it can be turned into
// specific type of Operation when be deserialized from bytes.
type OpContainer struct {
	OpType string          `json:"op_type"`
	OpData json.RawMessage `json:"op_data"`
}

type RegisterOperation struct {
	Id         string   `json:"id"`
	Address    string   `json:"address"`
	DataNodeId string   `json:"data_node_id"`
	ChunkIds   []string `json:"chunkIds"`
}

func (o RegisterOperation) Apply() (interface{}, error) {
	logrus.Infof("register, address: %s", o.Address)
	newSet := set.NewSet()
	for _, id := range o.ChunkIds {
		newSet.Add(id)
	}
	datanode := &DataNode{
		Id:            o.DataNodeId,
		status:        common.Cold,
		Address:       o.Address,
		Chunks:        newSet,
		IOLoad:        0,
		HeartbeatTime: time.Now(),
		Id:               o.DataNodeId,
		status:           common.Cold,
		Address:          o.Address,
		Chunks:           newSet,
		FutureSendChunks: make(map[ChunkSendInfo]int),
		IOLoad:           0,
		HeartbeatTime:    time.Now(),
	}
	AddDataNode(datanode)
	logrus.Infof("[Id=%s] Connected", o.DataNodeId)
	return o.DataNodeId, nil
}

type HeartbeatOperation struct {
	Id           string          `json:"id"`
	DataNodeId   string          `json:"data_node_id"`
	ChunkIds     []string        `json:"chunkIds"`
	IOLoad       int64           `json:"io_load"`
	SuccessInfos []ChunkSendInfo `json:"success_infos"`
	FailInfos    []ChunkSendInfo `json:"fail_infos"`
	IsReady      bool            `json:"is_ready"`
}

func (o HeartbeatOperation) Apply() (interface{}, error) {
	logrus.Infof("Heartbeat, id: %s", o.DataNodeId)
	nextChunkInfos, ok := UpdateDataNode4Heartbeat(o)
	if !ok {
		return nil, fmt.Errorf("datanode %s not exist", o.DataNodeId)
	}
	UpdateChunk4Heartbeat(o)
	return nextChunkInfos, nil
}

type AddOperation struct {
	Id           string                 `json:"id"`
	Path         string                 `json:"path"`
	FileName     string                 `json:"file_name"`
	Size         int64                  `json:"size"`
	FileNodeId   string                 `json:"file_node_id"`
	ChunkNum     int32                  `json:"chunk_num"`
	ChunkId      string                 `json:"chunk_id"`
	Infos        []util.ChunkSendResult `json:"infos"`
	FailChunkIds []string               `json:"fail_chunk_ids"`
	Stage        int                    `json:"stage"`
}

func (o AddOperation) Apply() (interface{}, error) {
	switch o.Stage {
	case common.CheckArgs:
		fileNode, stack, err := LockAndAddFileNode(o.FileNodeId, o.Path, o.FileName, o.Size, common.IsFile4AddFile)
		if err != nil {
			return nil, err
		}
		fileNodesMapLock.Lock()
		lockedFileNodes[fileNode.Id] = stack
		fileNodesMapLock.Unlock()
		rep := &pb.CheckArgs4AddReply{
			FileNodeId: fileNode.Id,
			ChunkNum:   int32(len(fileNode.Chunks)),
		}
		return rep, nil
	case common.GetDataNodes:
		dataNodes := BatchAllocateDataNodes(int(o.ChunkNum))
		chunks := make([]*Chunk, o.ChunkNum)
		dataNodeIds := make([]*pb.GetDataNodes4AddReply_Array, int(o.ChunkNum))
		dataNodeAdds := make([]*pb.GetDataNodes4AddReply_Array, int(o.ChunkNum))
		for i := 0; i < int(o.ChunkNum); i++ {
			chunkId := o.FileNodeId + common.ChunkIdDelimiter + strconv.Itoa(i)
			var (
				dataNodeIdSet = set.NewSet()
				dnIds         = make([]string, len(dataNodes[0]))
				dnAdds        = make([]string, len(dataNodes[0]))
			)
			for i, node := range dataNodes[i] {
				dataNodeIdSet.Add(node.Id)
				dnIds[i] = node.Id
				dnAdds[i] = node.Address
			}
			chunk := &Chunk{
				Id:               chunkId,
				dataNodes:        set.NewSet(),
				pendingDataNodes: dataNodeIdSet,
			}
			logrus.Debugf("Chunk index: %v, dnIds: %v, dnAdds: %v", i, dnIds, dnAdds)
			chunks[i] = chunk
			dataNodeIds[i] = &pb.GetDataNodes4AddReply_Array{
				Items: dnIds,
			}
			dataNodeAdds[i] = &pb.GetDataNodes4AddReply_Array{
				Items: dnAdds,
			}
		}
		BatchAddChunk(chunks)
		rep := &pb.GetDataNodes4AddReply{
			DataNodeIds:  dataNodeIds,
			DataNodeAdds: dataNodeAdds,
		}
		return rep, nil
	case common.UnlockDic:
		if o.FailChunkIds != nil {
			_, _ = EraseFileNode(o.Path)
			BatchClearPendingDataNodes(o.FailChunkIds)
		}
		BatchUpdatePendingDataNodes(o.Infos)
		BatchAddChunks(o.Infos)
		err := UnlockFileNodesById(o.FileNodeId, false)
		return nil, err
	default:
		return nil, nil
	}

}

type GetOperation struct {
	Id         string `json:"id"`
	Path       string `json:"path"`
	FileNodeId string `json:"file_node_id"`
	ChunkIndex int32  `json:"chunk_index"`
	ChunkId    string `json:"chunk_id"`
	Stage      int    `json:"stage"`
}

func (o GetOperation) Apply() (interface{}, error) {
	switch o.Stage {
	case common.CheckArgs:
		return CheckAndGetFileNode(o.Path)
	case common.GetDataNodes:
		chunkId := o.FileNodeId + common.ChunkIdDelimiter + strconv.FormatInt(int64(o.ChunkIndex), 10)
		chunk := GetChunk(chunkId)
		dataNodeIds, dataNodeAddrs := GetSortedDataNodeIds(chunk.dataNodes)
		rep := &pb.GetDataNodes4GetReply{
			DataNodeIds:   dataNodeIds,
			DataNodeAddrs: dataNodeAddrs,
			ChunkIndex:    o.ChunkIndex,
		}
		return rep, nil
	default:
		return nil, nil
	}
}

type MkdirOperation struct {
	Id       string `json:"id"`
	Path     string `json:"path"`
	FileName string `json:"file_name"`
}

func (o MkdirOperation) Apply() (interface{}, error) {
	return AddFileNode(o.Path, o.FileName, common.DirSize, false)
}

type MoveOperation struct {
	Id         string `json:"id"`
	SourcePath string `json:"source_path"`
	TargetPath string `json:"target_path"`
}

func (o MoveOperation) Apply() (interface{}, error) {
	return MoveFileNode(o.SourcePath, o.TargetPath)
}

type RemoveOperation struct {
	Id   string `json:"id"`
	Path string `json:"path"`
}

func (o RemoveOperation) Apply() (interface{}, error) {
	return RemoveFileNode(o.Path)
}

type ListOperation struct {
	Id   string `json:"id"`
	Path string `json:"path"`
}

func (o ListOperation) Apply() (interface{}, error) {
	fileNodes, err := ListFileNode(o.Path)
	return fileNode2FileInfo(fileNodes), err
}

type StatOperation struct {
	Id   string `json:"id"`
	Path string `json:"path"`
}

func (o StatOperation) Apply() (interface{}, error) {
	return StatFileNode(o.Path)
}

type RenameOperation struct {
	Id      string `json:"id"`
	Path    string `json:"path"`
	NewName string `json:"new_name"`
}

func (o RenameOperation) Apply() (interface{}, error) {
	return RenameFileNode(o.Path, o.NewName)
}

// fileNode2FileInfo converts []*FileNode to []*pb.FileInfo.
func fileNode2FileInfo(nodes []*FileNode) []*pb.FileInfo {
	files := make([]*pb.FileInfo, len(nodes))
	for i := 0; i < len(nodes); i++ {
		files[i] = &pb.FileInfo{
			FileName: nodes[i].FileName,
			IsFile:   nodes[i].IsFile,
		}
	}
	return files
}

type DegradeOperation struct {
	Id         string `json:"id"`
	DataNodeId string `json:"dataNodeId"`
	Stage      int    `json:"stage"`
}

func (o DegradeOperation) Apply() (interface{}, error) {
	DegradeDataNode(o.DataNodeId, o.Stage)
	return nil, nil
}

type AllocateChunksOperation struct {
	Id           string   `json:"id"`
	SenderPlan   []int    `json:"sender_plan"`
	ReceiverPlan []int    `json:"receiver_plan"`
	ChunkIds     []string `json:"chunk_ids"`
	DataNodeIds  []string `json:"data_node_ids"`
	BatchLen     int      `json:"batch_len"`
}

func (o AllocateChunksOperation) Apply() (interface{}, error) {
	ApplyAllocatePlan(o.SenderPlan, o.ReceiverPlan, o.ChunkIds, o.DataNodeIds, o.BatchLen)
	return nil, nil
}

type ExpandOperation struct {
	Id           string              `json:"id"`
	SenderPlan   map[string][]string `json:"sender_plan"`
	ReceiverPlan string              `json:"receiver_plan"`
	ChunkIds     []string            `json:"chunk_ids"`
}

func (e ExpandOperation) Apply() (interface{}, error) {
	logrus.Infof("Apply expand operation with dataNode #%s", e.ReceiverPlan)
	updateMapLock.Lock()
	for fromNodeId, targetChunks := range e.SenderPlan {
		fromNode := dataNodeMap[fromNodeId]
		for _, chunkId := range targetChunks {
			newFutureSendPlan := ChunkSendInfo{
				ChunkId:    chunkId,
				DataNodeId: e.ReceiverPlan,
				SendType:   common.Move,
			}
			fromNode.FutureSendChunks[newFutureSendPlan] = common.WaitToInform
		}
	}
	updateMapLock.Unlock()
	updateChunksLock.Lock()
	for _, chunkId := range e.ChunkIds {
		if chunk, ok := chunksMap[chunkId]; ok {
			chunk.pendingDataNodes.Add(e.ReceiverPlan)
		}
	}
	updateChunksLock.Unlock()
	return nil, nil
}
