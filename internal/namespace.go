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
)

var (
	// The root of the directory tree.
	root = &FileNode{
		Id:             util.GenerateUUIDString(),
		FileName:       rootFileName,
		childNodes:     make(map[string]*FileNode),
		updateNodeLock: &sync.RWMutex{},
	}
	// Store all locked nodes.
	// All nodes locked by an operation will be placed on a stack as the value of the map.
	// The id of the FileNode being operated is used as the key.
	unlockedFileNodes = make(map[string]*list.List)
	fileNodesMapLock  = &sync.Mutex{}
)

type FileNode struct {
	Id         string
	FileName   string
	parentNode *FileNode
	// all child node of this node, using FileName as key
	childNodes map[string]*FileNode
	// id of all Chunk in this file.
	Chunks []string
	// size of the file. Use bytes as the unit of measurement which means 1kb will be 1024.
	Size           int64
	IsFile         bool
	DelTime        *time.Time
	IsDel          bool
	updateNodeLock *sync.RWMutex
	LastLockTime   time.Time
}

func CheckAndGetFileNode(path string) (*FileNode, error) {
	fileNode, stack, isExist := getAndLockByPath(path, true)
	defer UnlockAllMutex(stack, true)
	if !isExist {
		return nil, fmt.Errorf("path not exist, path : %s", path)
	}
	return fileNode, nil
}

func getAndLockByPath(path string, isRead bool) (*FileNode, *list.List, bool) {
	currentNode := root
	path = strings.TrimRight(path, pathSplitString)
	fileNames := strings.Split(path, pathSplitString)
	stack := list.New()

	// fileNames的第一个是根目录，不存在子node包含根目录的节点，所以[1:]
	for _, name := range fileNames[1:] {
		currentNode.updateNodeLock.RLock()
		currentNode.LastLockTime = time.Now()
		stack.PushBack(currentNode)
		nextNode, exist := currentNode.childNodes[name]
		if !exist {
			return nil, stack, false
		}
		currentNode = nextNode
	}

	if isRead {
		currentNode.updateNodeLock.RLock()
	} else {
		currentNode.updateNodeLock.Lock()
	}
	stack.PushBack(currentNode)
	return currentNode, stack, true
}

func UnlockAllMutex(stack *list.List, isRead bool) {
	firstElement := stack.Back()
	firstNode := firstElement.Value.(*FileNode)
	if isRead {
		firstNode.updateNodeLock.RUnlock()
	} else {
		firstNode.updateNodeLock.Unlock()
	}
	stack.Remove(firstElement)

	for stack.Len() != 0 {
		element := stack.Back()
		node := element.Value.(*FileNode)
		node.updateNodeLock.RUnlock()
		stack.Remove(element)
	}
}

func AddFileNode(path string, filename string, size int64, isFile bool) (*FileNode, error) {
	fileNode, stack, isExist := getAndLockByPath(path, false)
	defer UnlockAllMutex(stack, false)
	if !isExist {
		return nil, fmt.Errorf("path not exist, path : %s", path)
	}
	id := util.GenerateUUIDString()
	newNode := &FileNode{
		Id:             id,
		FileName:       filename,
		parentNode:     fileNode,
		Size:           size,
		IsFile:         isFile,
		IsDel:          false,
		DelTime:        nil,
		updateNodeLock: &sync.RWMutex{},
		LastLockTime:   time.Now(),
	}
	if isFile {
		newNode.Chunks = initChunks(size, id)
	} else {
		newNode.childNodes = make(map[string]*FileNode)
	}
	fileNode.childNodes[filename] = newNode
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
		parentNode:     fileNode,
		Size:           size,
		IsFile:         isFile,
		IsDel:          false,
		DelTime:        nil,
		updateNodeLock: &sync.RWMutex{},
		LastLockTime:   time.Now(),
	}
	if isFile {
		newNode.Chunks = initChunks(size, id)
	} else {
		newNode.childNodes = make(map[string]*FileNode)
	}
	fileNode.childNodes[filename] = newNode
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
	defer UnlockAllMutex(stack, false)
	newParentNode, parentStack, isParentExist := getAndLockByPath(targetPath, false)
	defer UnlockAllMutex(parentStack, false)

	if !isExist {
		return nil, fmt.Errorf("current path not exist, path : %s", currentPath)
	} else if !isParentExist {
		return nil, fmt.Errorf("target path not exist, path : %s", targetPath)
	}
	if newParentNode.childNodes[fileNode.FileName] != nil {
		return nil, fmt.Errorf("target path already has file with the same name, filename : %s", fileNode.FileName)
	}

	newParentNode.childNodes[fileNode.FileName] = fileNode
	delete(fileNode.parentNode.childNodes, fileNode.FileName)
	fileNode.parentNode = newParentNode
	return fileNode, nil
}

func RemoveFileNode(path string) (*FileNode, error) {
	fileNode, stack, isExist := getAndLockByPath(path, false)
	if !isExist {
		// 假如目录树只有 /a/b  删除路径为 /a/b/c.txt
		// 则stack中只会对 ""  "a" "b"加读锁
		defer UnlockAllMutex(stack, true)
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
	defer UnlockAllMutex(stack, true)
	if !isExist {
		return nil, fmt.Errorf("path not exist, path : %s", path)
	}

	fileNodes := make([]*FileNode, len(fileNode.childNodes))
	nodeIndex := 0
	for _, n := range fileNode.childNodes {
		fileNodes[nodeIndex] = n
		nodeIndex++
	}
	return fileNodes, nil
}

func RenameFileNode(path string, newName string) (*FileNode, error) {
	fileNode, stack, isExist := getAndLockByPath(path, false)

	if !isExist {
		defer UnlockAllMutex(stack, true)
		return nil, fmt.Errorf("path not exist, path : %s", path)
	}

	defer UnlockAllMutex(stack, false)
	delete(fileNode.parentNode.childNodes, fileNode.FileName)
	fileNode.FileName = newName
	fileNode.parentNode.childNodes[fileNode.FileName] = fileNode
	if fileNode.IsDel {
		fileNode.IsDel = false
		fileNode.DelTime = nil
	}
	return fileNode, nil
}
