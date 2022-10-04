package internal

import (
	"bufio"
	"container/list"
	"encoding/json"
	"github.com/sirupsen/logrus"
	"io"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"tinydfs-base/common"
	"tinydfs-base/protocol/pb"
	"tinydfs-base/util"
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

const (
	Operation_Add    = "Add"
	Operation_Get    = "Get"
	Operation_Remove = "Remove"
	Operation_Move   = "Move"
	Operation_Rename = "Rename"
	Operation_List   = "List"
	Operation_Mkdir  = "Mkdir"
	Operation_Stat   = "Stat"
	Operation_Finish = "Finish"
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

func OperationAdd(des string, isFile bool, fileName string, size int64) *pb.OperationArgs {
	return &pb.OperationArgs{
		Uuid:     util.GenerateUUIDString(),
		Type:     Operation_Add,
		Des:      des,
		IsFile:   isFile,
		FileName: fileName,
		Size:     size,
		IsFinish: false,
	}
}

func OperationRemove(des string) *pb.OperationArgs {
	return &pb.OperationArgs{
		Uuid: util.GenerateUUIDString(),
		Type: Operation_Remove,
		Des:  des,
	}
}

func OperationGet(path string) *pb.OperationArgs {
	return &pb.OperationArgs{
		Uuid: util.GenerateUUIDString(),
		Type: Operation_Get,
		Des:  path,
	}
}

func OperationRename(src, des string) *pb.OperationArgs {
	return &pb.OperationArgs{
		Uuid: util.GenerateUUIDString(),
		Type: Operation_Rename,
		Src:  src,
		Des:  des,
	}
}

func OperationMove(src, des string) *pb.OperationArgs {
	return &pb.OperationArgs{
		Uuid: util.GenerateUUIDString(),
		Type: Operation_Move,
		Src:  src,
		Des:  des,
	}
}

func OperationList(des string) *pb.OperationArgs {
	return &pb.OperationArgs{
		Uuid: util.GenerateUUIDString(),
		Type: Operation_List,
		Des:  des,
	}
}

func OperationMkdir(des string, filename string) *pb.OperationArgs {
	return &pb.OperationArgs{
		Uuid:     util.GenerateUUIDString(),
		Type:     Operation_Mkdir,
		Des:      des,
		FileName: filename,
	}
}

func OperationStat(path string) *pb.OperationArgs {
	return &pb.OperationArgs{
		Uuid: util.GenerateUUIDString(),
		Type: Operation_Stat,
		Des:  path,
	}
}

func FinishOperation(uuid string) *pb.OperationArgs {
	return &pb.OperationArgs{
		Uuid:     uuid,
		Type:     Operation_Finish,
		IsFinish: true,
	}
}

func Merge2Root(root *FileNode, ops []*pb.OperationArgs) {
	for _, op := range ops {
		switch op.Type {
		case Operation_Add:
			Add2Root(root, op)
		case Operation_Remove:
			Remove2Root(root, op)
		case Operation_Rename:
			Rename2Root(root, op)
		case Operation_Move:
			Move2Root(root, op)
		case Operation_Mkdir:
			Mkdir2Root(root, op)
		default:
			logrus.Info("Operation Type ", op.Type)
		}
	}
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

func Add2Root(root *FileNode, op *pb.OperationArgs) {
	parent := root.getFileNodeByPath(op.Des)
	newNode := &FileNode{
		Id:             op.Uuid,
		FileName:       op.FileName,
		ParentNode:     parent,
		Size:           op.Size,
		IsFile:         true,
		IsDel:          false,
		Chunks:         initChunks(op.Size, op.Uuid),
		DelTime:        nil,
		UpdateNodeLock: &sync.RWMutex{},
	}
	parent.ChildNodes[newNode.FileName] = newNode
}

func Rename2Root(root *FileNode, op *pb.OperationArgs) {
	fileNode := root.getFileNodeByPath(op.Src)
	delete(fileNode.ParentNode.ChildNodes, fileNode.FileName)
	fileNode.FileName = op.Des
	fileNode.ParentNode.ChildNodes[fileNode.FileName] = fileNode
	if fileNode.IsDel {
		fileNode.IsDel = false
		fileNode.DelTime = nil
	}
}

func Remove2Root(root *FileNode, op *pb.OperationArgs) {
	fileNode := root.getFileNodeByPath(op.Des)
	fileNode.FileName = deleteFilePrefix + fileNode.FileName
	fileNode.IsDel = true
	delTime := time.Now()
	fileNode.DelTime = &(delTime)
}

func Move2Root(root *FileNode, op *pb.OperationArgs) {
	fileNode := root.getFileNodeByPath(op.Src)
	newParentNode := root.getFileNodeByPath(op.Des)
	newParentNode.ChildNodes[fileNode.FileName] = fileNode
	delete(fileNode.ParentNode.ChildNodes, fileNode.FileName)
	fileNode.ParentNode = newParentNode
}

func Mkdir2Root(root *FileNode, op *pb.OperationArgs) {
	parent := root.getFileNodeByPath(op.Des)
	newNode := &FileNode{
		Id:             op.Uuid,
		FileName:       op.FileName,
		ParentNode:     parent,
		Size:           0,
		IsFile:         false,
		IsDel:          false,
		ChildNodes:     map[string]*FileNode{},
		DelTime:        nil,
		UpdateNodeLock: &sync.RWMutex{},
	}
	if parent == nil {
		logrus.Infof("parent is nil")
	}
	if parent.ChildNodes == nil {
		logrus.Infof("child of parent is nil")
	}
	parent.ChildNodes[newNode.FileName] = newNode
}

//TODO 感觉通过设计模式包装。读edit和读fsimage可以复用
//ReadLogLines read the `path` log and find all of the finished operations
func ReadLogLines(path string) []*pb.OperationArgs {
	f, err := os.Open(path)
	defer f.Close()
	if err != nil {
		logrus.Warnf("Open edits file failed.Error detail %v\n", err)
		//TODO 需要额外处理
		return nil
	}
	buf := bufio.NewScanner(f)
	res := make([]*pb.OperationArgs, 0)
	opMap := make(map[string]*pb.OperationArgs)
	for buf.Scan() {
		line := buf.Text()
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		line = strings.ReplaceAll(line, "\\", "")
		left := strings.Index(line, "{")
		right := strings.Index(line, "}")
		op := &pb.OperationArgs{}
		util.Unmarshal(line[left:right+1], op)
		// Ignore the non-update operation
		if op.Type == Operation_Stat || op.Type == Operation_List {
			continue
		}
		// Map is used to filter which operation is finished
		_, ok := opMap[op.Uuid]
		if !ok {
			opMap[op.Uuid] = op
		} else {
			res = append(res, opMap[op.Uuid])
		}
	}
	return res
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

// ReadRootLines returns map whose key is the id of the filenode rather than filename
func ReadRootLines(path string) map[string]*FileNode {
	f, err := os.Open(path)
	defer f.Close()
	if err != nil {
		logrus.Errorf("Open fsimage file failed: %v\n", err)
		//TODO 需要额外处理
		return nil
	}
	buf := bufio.NewScanner(f)
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

//RootSerialize put the shadow directory onto the file storage
func RootSerialize(root *FileNode) {
	if root == nil {
		return
	}
	// Write root level-order in the fsimage
	file, err := os.OpenFile(common.DirectoryFileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	defer file.Close()
	if err != nil {
		logrus.Warnf("Open %s failed.Error Detail %s\n", common.DirectoryFileName, err)
		return
	}
	queue := list.New()
	queue.PushBack(root)
	for queue.Len() != 0 {
		cur := queue.Front()
		queue.Remove(cur)
		node, ok := cur.Value.(*FileNode)
		if !ok {
			logrus.Warnf("Element2FileNode failed\n")
		}
		_, err = file.WriteString(node.String())
		if err != nil {
			logrus.Errorf("Write String failed.Error Detail %s\n", err)
		}
		for _, child := range node.ChildNodes {
			queue.PushBack(child)
		}
	}
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
