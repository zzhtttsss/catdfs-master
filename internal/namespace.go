package internal

import (
	"container/list"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"
	"tinydfs-base/util"
)

const (
	kb               = 1024
	mb               = 1048576
	chunkByteNum     = 1024
	chunkSize        = 1
	rootFileName     = ""
	pathSplitString  = "/"
	deleteFilePrefix = "delete"
	RootFileName     = ".\\fsimage.txt"
	TimeFormat       = "2006-01-02-15.04.05"
)

var (
	// The root of the directory tree.
	root = &FileNode{
		Id:             util.GenerateUUIDString(),
		FileName:       rootFileName,
		ChildNodes:     make(map[string]*FileNode),
		UpdateNodeLock: &sync.RWMutex{},
	}
	// Store all locked nodes.
	// All nodes locked by an operation will be placed on a stack as the value of the map.
	// The id of the FileNode being operated is used as the key.
	//TODO 删除操作
	unlockedFileNodes = make(map[string]*list.List)
	fileNodesMapLock  = &sync.Mutex{}
)

type FileNode struct {
	Id         string
	FileName   string
	ParentNode *FileNode
	// all child node of this node, using FileName as key
	ChildNodes map[string]*FileNode
	// id of all Chunk in this file.
	Chunks []string
	// size of the file. Use bytes as the unit of measurement which means 1kb will be 1024.
	Size           int64
	IsFile         bool
	DelTime        *time.Time
	IsDel          bool
	UpdateNodeLock *sync.RWMutex
	LastLockTime   time.Time
}

func (f *FileNode) String() string {
	res := strings.Builder{}
	if f.ParentNode == nil {
		res.WriteString(fmt.Sprintf("%s %s %s %d %s %d %v %v %v %s\n",
			f.Id, f.FileName, "-1", len(f.ChildNodes), f.Chunks,
			f.Size, f.IsFile, f.DelTime, f.IsDel, f.LastLockTime.Format(TimeFormat)))
	} else {
		res.WriteString(fmt.Sprintf("%s %s %s %d %s %d %v %v %v %s\n",
			f.Id, f.FileName, f.ParentNode.Id, len(f.ChildNodes), f.Chunks,
			f.Size, f.IsFile, f.DelTime, f.IsDel, f.LastLockTime.Format(TimeFormat)))
	}

	return res.String()
}

func CheckAndGetFileNode(path string) (*FileNode, error) {
	fileNode, stack, isExist := getAndLockByPath(path, true)
	if !isExist {
		return nil, fmt.Errorf("path not exist, path : %s", path)
	}
	defer UnlockAllMutex(stack, true)
	return fileNode, nil
}

func getAndLockByPath(path string, isRead bool) (*FileNode, *list.List, bool) {
	currentNode := root
	path = strings.Trim(path, pathSplitString)
	fileNames := strings.Split(path, pathSplitString)
	stack := list.New()

	for _, name := range fileNames {
		currentNode.UpdateNodeLock.RLock()
		currentNode.LastLockTime = time.Now()
		stack.PushBack(currentNode)
		nextNode, exist := currentNode.ChildNodes[name]
		if !exist {
			UnlockAllMutex(stack, true)
			return nil, stack, false
		}
		currentNode = nextNode
	}

	if isRead {
		currentNode.UpdateNodeLock.RLock()
	} else {
		currentNode.UpdateNodeLock.Lock()
	}
	stack.PushBack(currentNode)
	return currentNode, stack, true
}

func UnlockAllMutex(stack *list.List, isRead bool) {
	firstElement := stack.Back()
	firstNode := firstElement.Value.(*FileNode)
	if isRead {
		firstNode.UpdateNodeLock.RUnlock()
	} else {
		firstNode.UpdateNodeLock.Unlock()
	}
	stack.Remove(firstElement)

	for stack.Len() != 0 {
		element := stack.Back()
		node := element.Value.(*FileNode)
		node.UpdateNodeLock.RUnlock()
		stack.Remove(element)
	}
}

func AddFileNode(path string, filename string, size int64, isFile bool) (*FileNode, error) {
	fileNode, stack, isExist := getAndLockByPath(path, false)
	if !isExist {
		return nil, fmt.Errorf("path not exist, path : %s", path)
	}
	defer UnlockAllMutex(stack, false)

	id := util.GenerateUUIDString()
	newNode := &FileNode{
		Id:             id,
		FileName:       filename,
		ParentNode:     fileNode,
		Size:           size,
		IsFile:         isFile,
		IsDel:          false,
		DelTime:        nil,
		UpdateNodeLock: &sync.RWMutex{},
		LastLockTime:   time.Now(),
	}
	if isFile {
		newNode.Chunks = initChunks(size, id)
	} else {
		newNode.ChildNodes = make(map[string]*FileNode)
	}
	fileNode.ChildNodes[filename] = newNode
	return newNode, nil
}

func LockAndAddFileNode(path string, filename string, size int64, isFile bool) (*FileNode, *list.List, error) {
	fileNode, stack, isExist := getAndLockByPath(path, false)
	if !isExist {
		return nil, nil, fmt.Errorf("path not exist, path : %s", path)
	}

	id := util.GenerateUUIDString()
	newNode := &FileNode{
		Id:             id,
		FileName:       filename,
		ParentNode:     fileNode,
		Size:           size,
		IsFile:         isFile,
		IsDel:          false,
		DelTime:        nil,
		UpdateNodeLock: &sync.RWMutex{},
		LastLockTime:   time.Now(),
	}
	if isFile {
		newNode.Chunks = initChunks(size, id)
	} else {
		newNode.ChildNodes = make(map[string]*FileNode)
	}
	fileNode.ChildNodes[filename] = newNode
	return newNode, stack, nil
}

func initChunks(size int64, id string) []string {
	nums := int(math.Ceil(float64(size) / float64(chunkSize) / float64(chunkByteNum)))
	chunks := make([]string, nums)
	for i := 0; i < len(chunks); i++ {
		chunks[i] = id + strconv.Itoa(i)
	}
	return chunks
}

func MoveFileNode(currentPath string, targetPath string) (*FileNode, error) {
	fileNode, stack, isExist := getAndLockByPath(currentPath, false)
	newParentNode, parentStack, isParentExist := getAndLockByPath(targetPath, false)
	if !isExist {
		return nil, fmt.Errorf("current path not exist, path : %s", currentPath)
	}
	defer UnlockAllMutex(stack, false)
	if !isParentExist {
		return nil, fmt.Errorf("target path not exist, path : %s", targetPath)
	}
	defer UnlockAllMutex(parentStack, false)
	if newParentNode.ChildNodes[fileNode.FileName] != nil {
		return nil, fmt.Errorf("target path already has file with the same name, filename : %s", fileNode.FileName)
	}

	newParentNode.ChildNodes[fileNode.FileName] = fileNode
	delete(fileNode.ParentNode.ChildNodes, fileNode.FileName)
	fileNode.ParentNode = newParentNode
	return fileNode, nil
}

func RemoveFileNode(path string) (*FileNode, error) {
	fileNode, stack, isExist := getAndLockByPath(path, false)
	if !isExist {
		return nil, fmt.Errorf("path not exist, path : %s", path)
	}
	defer UnlockAllMutex(stack, false)

	fileNode.FileName = deleteFilePrefix + fileNode.FileName
	fileNode.IsDel = true
	delTime := time.Now()
	fileNode.DelTime = &(delTime)
	return fileNode, nil
}

func ListFileNode(path string) ([]*FileNode, error) {
	fileNode, stack, isExist := getAndLockByPath(path, true)
	if !isExist {
		return nil, fmt.Errorf("path not exist, path : %s", path)
	}
	defer UnlockAllMutex(stack, true)

	fileNodes := make([]*FileNode, len(fileNode.ChildNodes))
	nodeIndex := 0
	for _, n := range fileNode.ChildNodes {
		fileNodes[nodeIndex] = n
		nodeIndex++
	}
	return fileNodes, nil
}

func RenameFileNode(path string, newName string) (*FileNode, error) {
	fileNode, stack, isExist := getAndLockByPath(path, false)
	if !isExist {
		return nil, fmt.Errorf("path not exist, path : %s", path)
	}
	defer UnlockAllMutex(stack, false)

	delete(fileNode.ParentNode.ChildNodes, fileNode.FileName)
	fileNode.FileName = newName
	fileNode.ParentNode.ChildNodes[fileNode.FileName] = fileNode
	if fileNode.IsDel {
		fileNode.IsDel = false
		fileNode.DelTime = nil
	}
	return fileNode, nil
}
