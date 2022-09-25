package internal

import (
	"bufio"
	"container/list"
	"github.com/sirupsen/logrus"
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

type Operation interface {
	Apply() error

	Merge2root()

	Conv2Proto() *pb.OperationArgs
}

type MkdirOperation struct {
	Id       string `json:"id"`
	Des      string `json:"des"`
	FileName string `json:"file_name"`
	Type     string `json:"type"`
}

func (ap MkdirOperation) Apply() error {
	err := DoCheckAndMkdir(ap.Des, ap.FileName)
	if err != nil {
		return err
	}
	return nil
}

func (ap MkdirOperation) Merge2root() {
	parent := root.getFileNodeByPath(ap.Des)
	newNode := &FileNode{
		Id:             ap.Id,
		FileName:       ap.FileName,
		ParentNode:     parent,
		Size:           common.DirSize,
		IsFile:         false,
		IsDel:          false,
		UpdateNodeLock: &sync.RWMutex{},
	}
	newNode.ChildNodes = make(map[string]*FileNode)
	parent.ChildNodes[newNode.FileName] = newNode
}

func (ap MkdirOperation) Conv2Proto() *pb.OperationArgs {
	return &pb.OperationArgs{
		Uuid:     ap.Id,
		Type:     Operation_Mkdir,
		Des:      ap.Des,
		IsFile:   false,
		FileName: ap.FileName,
		Size:     common.DirSize,
		IsFinish: false,
	}
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
