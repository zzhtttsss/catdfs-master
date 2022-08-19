package model

import (
	"container/list"
	"fmt"
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

type Namespace struct {
	Root *FileNode
	//LockRule map[string]bool
}

func CreateNamespace() *Namespace {
	return &Namespace{
		Root: CreateRootNode(),
	}
}

type FileNode struct {
	Id         string
	FileName   string
	parentNode *FileNode
	childNodes map[string]*FileNode
	Chunks     []string
	Size       int
	IsFile     bool
	DelTime    *time.Time
	IsDel      bool
	mu         *sync.RWMutex
}

func CreateRootNode() *FileNode {
	return &FileNode{
		Id:         util.GenerateUUIDString(),
		FileName:   rootFileName,
		childNodes: make(map[string]*FileNode),
		mu:         &sync.RWMutex{},
	}
}

func (node FileNode) CheckAndGet(path string) (*FileNode, error) {
	fileNode, stack, isExist := getAndLockByPath(&node, path, true)
	defer unlockAllMutex(stack, true)
	if !isExist {
		return nil, fmt.Errorf("path not exist, path : %s", path)
	}
	return fileNode, nil
}

func getAndLockByPath(node *FileNode, path string, isRead bool) (*FileNode, *list.List, bool) {
	currentNode := node
	fileNames := strings.Split(path, pathSplitString)
	stack := list.New()

	for _, name := range fileNames {
		currentNode.mu.RLock()
		stack.PushBack(currentNode)
		nextNode, exist := currentNode.childNodes[name]
		if !exist {
			return nil, stack, false
		}
		currentNode = nextNode
	}

	if isRead {
		currentNode.mu.RLock()
	} else {
		currentNode.mu.Lock()
	}
	stack.PushBack(currentNode)
	return currentNode, stack, true
}

func unlockAllMutex(stack *list.List, isRead bool) {
	firstElement := stack.Back()
	firstNode := firstElement.Value.(FileNode)
	if isRead {
		firstNode.mu.RUnlock()
	} else {
		firstNode.mu.Unlock()
	}
	stack.Remove(firstElement)

	for stack.Len() != 0 {
		element := stack.Back()
		node := element.Value.(FileNode)
		node.mu.RUnlock()
		stack.Remove(element)
	}
}

func (node FileNode) Add(path string, filename string, size int, isFile bool) (*FileNode, error) {
	var (
		chunks     []string
		childNodes map[string]*FileNode
	)

	fileNode, stack, isExist := getAndLockByPath(&node, path, false)
	defer unlockAllMutex(stack, false)
	if !isExist {
		return nil, fmt.Errorf("path not exist, path : %s", path)
	}
	if isFile {
		chunks = make([]string, size/(chunkSize*chunkByteNum))
	} else {
		childNodes = make(map[string]*FileNode)
	}
	newNode := &FileNode{
		Id:         util.GenerateUUIDString(),
		FileName:   filename,
		parentNode: fileNode,
		childNodes: childNodes,
		Chunks:     chunks,
		Size:       size,
		IsFile:     isFile,
		IsDel:      false,
		DelTime:    nil,
		mu:         &sync.RWMutex{},
	}
	fileNode.childNodes[filename] = newNode
	return newNode, nil
}

func (node FileNode) Move(currentPath string, targetPath string) (*FileNode, error) {
	fileNode, stack, isExist := getAndLockByPath(&node, currentPath, false)
	defer unlockAllMutex(stack, false)
	newParentNode, parentStack, isParentExist := getAndLockByPath(&node, targetPath, false)
	defer unlockAllMutex(parentStack, false)

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

func (node FileNode) Remove(path string) (*FileNode, error) {
	fileNode, stack, isExist := getAndLockByPath(&node, path, false)
	defer unlockAllMutex(stack, false)
	if !isExist {
		return nil, fmt.Errorf("path not exist, path : %s", path)
	}
	fileNode.FileName = deleteFilePrefix + util.GenerateUUIDString()
	fileNode.IsDel = true
	delTime := time.Now()
	fileNode.DelTime = &delTime
	return fileNode, nil
}

func (node FileNode) List(path string) ([]*FileNode, error) {
	fileNode, stack, isExist := getAndLockByPath(&node, path, true)
	defer unlockAllMutex(stack, true)
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

func (node FileNode) Rename(path string, newName string) (*FileNode, error) {
	fileNode, stack, isExist := getAndLockByPath(&node, path, false)
	defer unlockAllMutex(stack, false)
	if !isExist {
		return nil, fmt.Errorf("path not exist, path : %s", path)
	}

	delete(fileNode.parentNode.childNodes, fileNode.FileName)
	fileNode.FileName = newName
	fileNode.parentNode.childNodes[fileNode.FileName] = fileNode
	if fileNode.IsDel {
		fileNode.IsDel = false
		fileNode.DelTime = nil
	}
	return fileNode, nil
}
