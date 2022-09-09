package internal

import (
	"github.com/sirupsen/logrus"
	"strings"
	"sync"
	"time"
	"tinydfs-base/protocol/pb"
	"tinydfs-base/util"
)

const (
	Operation_Add    = "Add"
	Operation_Remove = "Remove"
	Operation_Move   = "Move"
	Operation_Rename = "Rename"
	Operation_List   = "List"
	Operation_Mkdir  = "Mkdir"
)

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

func OperationMkdir(des string) *pb.OperationArgs {
	return &pb.OperationArgs{
		Uuid: util.GenerateUUIDString(),
		Type: Operation_Mkdir,
		Des:  des,
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
	parent.ChildNodes[newNode.FileName] = newNode
}
