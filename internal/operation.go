package internal

import (
	"encoding/json"
	"fmt"
	set "github.com/deckarep/golang-set"
	"github.com/sirupsen/logrus"
	"reflect"
	"strconv"
	"strings"
	"time"
	"tinydfs-base/common"
	"tinydfs-base/protocol/pb"
)

const (
	idIdx = iota
	fileNameIdx
	parentIdIdx
	childrenIdx
	chunksIdIdx
	sizeIdx
	isFileIdx
	delTimeIdx
	isDelIdx
	lastLockTimeIdx
)

var (
	OperationTypes = make(map[string]reflect.Type)
)

func init() {
	OperationTypes[common.OperationRegister] = reflect.TypeOf(RegisterOperation{})
	OperationTypes[common.OperationHeartbeat] = reflect.TypeOf(HeartbeatOperation{})
	OperationTypes[common.OperationAdd] = reflect.TypeOf(AddOperation{})
	OperationTypes[common.OperationGet] = reflect.TypeOf(GetOperation{})
	OperationTypes[common.OperationMkdir] = reflect.TypeOf(MkdirOperation{})
	OperationTypes[common.OperationMove] = reflect.TypeOf(MoveOperation{})
	OperationTypes[common.OperationRemove] = reflect.TypeOf(RemoveOperation{})
	OperationTypes[common.OperationList] = reflect.TypeOf(ListOperation{})
	OperationTypes[common.OperationStat] = reflect.TypeOf(StatOperation{})
	OperationTypes[common.OperationRename] = reflect.TypeOf(RenameOperation{})
}

type Operation interface {
	Apply() (interface{}, error)
}

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
	Id         string `json:"id"`
	DataNodeId string `json:"data_node_id"`
}

func (o HeartbeatOperation) Apply() (interface{}, error) {
	logrus.Infof("heartbeat, id: %s", o.DataNodeId)
	if dataNode := GetDataNode(o.DataNodeId); dataNode != nil {
		dataNode.HeartbeatTime = time.Now()
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
		fileNodesMapLock.Lock()
		lockedFileNodes[fileNode.Id] = stack
		fileNodesMapLock.Unlock()
		if err != nil {
			return nil, err
		}
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

func (f *FileNode) getFileNodeByPath(path string) *FileNode {
	currentNode := f
	path = strings.Trim(path, pathSplitString)
	fileNames := strings.Split(path, pathSplitString)
	if path == f.FileName {
		return f
	}
	for _, name := range fileNames {
		nextNode, _ := currentNode.ChildNodes[name]
		currentNode = nextNode
	}
	return currentNode
}
