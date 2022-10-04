package internal

import (
	"bufio"
	"encoding/json"
	"io"
	"reflect"
	"strconv"
	"strings"
	"sync"
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
		fileNode, stack, err := LockAndAddFileNode(o.Path, o.FileName, o.Size, common.IsFile4AddFile)
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

// ReadSnapshotLines returns map whose key is the id of the filenode rather than filename
func ReadSnapshotLines(r io.ReadCloser) map[string]*FileNode {
	buf := bufio.NewScanner(r)
	res := map[string]*FileNode{}
	for buf.Scan() {
		line := buf.Text()
		data := strings.Split(line, "$")
		childrenLen := len(data[childrenIdx])
		childrenData := data[childrenIdx][1 : childrenLen-1]
		var children map[string]*FileNode
		if childrenData == "" {
			children = nil
		} else {
			children = map[string]*FileNode{}
			for _, childId := range strings.Split(childrenData, " ") {
				children[childId] = &FileNode{
					Id: childId,
				}
			}
		}
		chunksLen := len(data[chunksIdIdx])
		chunksData := data[chunksIdIdx][1 : chunksLen-1]
		var chunks []string
		if chunksData == "" {
			//TODO NPE隐患
			chunks = nil
		} else {
			chunks = strings.Split(chunksData, " ")
		}
		size, _ := strconv.Atoi(data[sizeIdx])
		isFile, _ := strconv.ParseBool(data[isFileIdx])
		delTime, _ := time.Parse(common.LogFileTimeFormat, data[delTimeIdx])
		var delTimePtr *time.Time
		if data[delTimeIdx] == "<nil>" {
			delTimePtr = nil
		} else {
			delTimePtr = &delTime
		}
		isDel, _ := strconv.ParseBool(data[isDelIdx])
		lastLockTime, _ := time.Parse(common.LogFileTimeFormat, data[lastLockTimeIdx])
		fn := &FileNode{
			Id:       data[idIdx],
			FileName: data[fileNameIdx],
			ParentNode: &FileNode{
				Id: data[parentIdIdx],
			},
			ChildNodes:     children,
			Chunks:         chunks,
			Size:           int64(size),
			IsFile:         isFile,
			DelTime:        delTimePtr,
			IsDel:          isDel,
			UpdateNodeLock: &sync.RWMutex{},
			LastLockTime:   lastLockTime,
		}
		res[fn.Id] = fn
	}
	return res
}

// RootDeserialize reads the fsimage.txt and rebuild the directory.
func RootDeserialize(rootMap map[string]*FileNode) *FileNode {
	// Look for root
	newRoot := &FileNode{}
	for _, r := range rootMap {
		if r.ParentNode.Id == common.MinusOneString {
			newRoot = r
			break
		}
	}
	buildTree(newRoot, rootMap)
	newRoot.ParentNode = nil
	return newRoot
}

func buildTree(cur *FileNode, nodeMap map[string]*FileNode) {
	if cur == nil {
		return
	}
	// id is the key of cur.ChildNodes which is uuid
	ids := make([]string, 0)
	for id, _ := range cur.ChildNodes {
		ids = append(ids, id)
	}
	for _, id := range ids {
		node := nodeMap[id]
		delete(cur.ChildNodes, id)
		cur.ChildNodes[node.FileName] = node
		node.ParentNode = cur
		buildTree(node, nodeMap)
	}
}
