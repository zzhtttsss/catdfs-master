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
)

var (
	// OpTypeMap is used to include all types of Operation. When implementing a new type of Operation, we should put
	// <name of operation, type of operation> into this map.
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
	OpTypeMap[common.OperationShrink] = reflect.TypeOf(ShrinkOperation{})
	OpTypeMap[common.OperationExpand] = reflect.TypeOf(ExpandOperation{})
	OpTypeMap[common.OperationDeregister] = reflect.TypeOf(DeregisterOperation{})
}

// Operation represents requests to make changes to metadata.
type Operation interface {
	// Apply will perform modifications to the metadata. Calling it in the MasterFSM can ensure
	// the consistency of the metadata modification of the master cluster.
	Apply() (interface{}, error)
}

// OpContainer is used to encapsulate Operation so that it can be turned into specific type of Operation
// when be deserialized from bytes.
type OpContainer struct {
	OpType string          `json:"op_type"`
	OpData json.RawMessage `json:"op_data"`
}

type RegisterOperation struct {
	Id         string `json:"id"`
	Address    string `json:"address"`
	DataNodeId string `json:"data_node_id"`
}

func (o RegisterOperation) Apply() (interface{}, error) {
	logrus.Infof("register, address: %s", o.Address)
	datanode := &DataNode{
		Id:            o.DataNodeId,
		status:        common.Alive,
		Address:       o.Address,
		Chunks:        set.NewSet(),
		Leases:        set.NewSet(),
		HeartbeatTime: time.Now(),
	}

	AddDataNode(datanode)
	Adjust4Add(datanode)
	logrus.Infof("[Id=%s] Connected", o.DataNodeId)
	return o.DataNodeId, nil
}

type HeartbeatOperation struct {
	Id         string   `json:"id"`
	DataNodeId string   `json:"data_node_id"`
	ChunkIds   []string `json:"chunkIds"`
}

func (o HeartbeatOperation) Apply() (interface{}, error) {
	logrus.Infof("heartbeat, id: %s", o.DataNodeId)
	if dataNode := GetDataNode(o.DataNodeId); dataNode != nil {
		dataNode.HeartbeatTime = time.Now()
		dataNode.status = common.Alive
		return nil, nil
	}
	return nil, fmt.Errorf("datanode %s not exist", o.DataNodeId)
}

type AddOperation struct {
	Id         string `json:"id"`
	Path       string `json:"path"`
	FileName   string `json:"file_name"`
	Size       int64  `json:"size"`
	FileNodeId string `json:"file_node_id"`
	ChunkIndex int32  `json:"chunk_index"`
	ChunkId    string `json:"chunk_id"`
	Stage      int    `json:"stage"`
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
		chunkId := o.FileNodeId + common.ChunkIdDelimiter + strconv.Itoa(int(o.ChunkIndex))
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
		rep := &pb.GetDataNodes4AddReply{
			DataNodes:   dataNodeAddrs,
			PrimaryNode: primaryNode.Address,
		}
		return rep, nil
	case common.ReleaseLease:
		chunk := GetChunk(o.ChunkId)
		err := ReleaseLease(chunk.primaryNode, o.ChunkId)
		return nil, err
	case common.UnlockDic:
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
		dataNodeIds := make([]string, len(chunk.dataNodes))
		dataNodeAddrs := make([]string, len(chunk.dataNodes))
		for i, nodeId := range chunk.dataNodes {
			dataNode := GetDataNode(nodeId)
			dataNodeIds[i] = dataNode.Id
			dataNodeAddrs[i] = dataNode.Address
		}
		rep := &pb.GetDataNodes4GetReply{
			DataNodeIds:   dataNodeIds,
			DataNodeAddrs: dataNodeAddrs,
			ChunkIndex:    o.ChunkIndex,
		}
		return rep, nil
	case common.ReleaseLease:
		chunk := GetChunk(o.ChunkId)
		err := ReleaseLease(chunk.primaryNode, o.ChunkId)
		return nil, err
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
	logrus.Infof("mkdir, path: %s, filename: %s", o.Path, o.FileName)
	return AddFileNode(o.Path, o.FileName, common.DirSize, false)
}

type MoveOperation struct {
	Id         string `json:"id"`
	SourcePath string `json:"source_path"`
	TargetPath string `json:"target_path"`
}

func (o MoveOperation) Apply() (interface{}, error) {
	logrus.Infof("move, SourcePath: %s, TargetPath: %s", o.SourcePath, o.TargetPath)
	return MoveFileNode(o.SourcePath, o.TargetPath)
}

type RemoveOperation struct {
	Id   string `json:"id"`
	Path string `json:"path"`
}

func (o RemoveOperation) Apply() (interface{}, error) {
	logrus.Infof("remove, path: %s", o.Path)
	return RemoveFileNode(o.Path)
}

type ListOperation struct {
	Id   string `json:"id"`
	Path string `json:"path"`
}

func (o ListOperation) Apply() (interface{}, error) {
	logrus.Infof("list, path: %s", o.Path)
	fileNodes, err := ListFileNode(o.Path)
	return fileNode2FileInfo(fileNodes), err
}

type StatOperation struct {
	Id   string `json:"id"`
	Path string `json:"path"`
}

func (o StatOperation) Apply() (interface{}, error) {
	logrus.Infof("stat, path: %s", o.Path)
	return StatFileNode(o.Path)
}

type RenameOperation struct {
	Id      string `json:"id"`
	Path    string `json:"path"`
	NewName string `json:"new_name"`
}

func (o RenameOperation) Apply() (interface{}, error) {
	logrus.Infof("rename, path: %s, newName: %s", o.Path, o.NewName)
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

type DeregisterOperation struct {
	Id         string `json:"id"`
	DataNodeId string `json:"dataNodeId"`
}

func (d DeregisterOperation) Apply() (interface{}, error) {
	RemoveDataNode(d.DataNodeId)
	return nil, nil
}

type ShrinkOperation struct {
	Id            string `json:"id"`
	ShrinkChunkId string `json:"shrinkChunkId"`
	FromDataNode  string `json:"fromDataNode"`
	ToDataNodes   string `json:"toDataNode"`
}

func (o ShrinkOperation) Apply() (interface{}, error) {
	//TODO implement me
	panic("implement me")
}

type ExpandOperation struct {
}
