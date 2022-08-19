package model

import (
	"container/list"
	"fmt"
	"strings"
	"sync"
)

const (
	kb              = 1024
	mb              = 1048576
	chunkByteNUm    = 1024
	chunkSize       = 1
	rootFileName    = ""
	pathSplitString = "/"
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
	FileName   string
	ParentNode *FileNode
	ChildNodes map[string]*FileNode
	Chunks     []string
	Size       int
	IsFile     bool
	mu         *sync.RWMutex
}

func CreateRootNode() *FileNode {
	return &FileNode{
		FileName:   rootFileName,
		ParentNode: nil,
		ChildNodes: make(map[string]*FileNode),
		Chunks:     nil,
		IsFile:     false,
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
		nextNode, exist := currentNode.ChildNodes[name]
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
		chunks = make([]string, size/(chunkSize*chunkByteNUm))
	} else {
		childNodes = make(map[string]*FileNode)
	}

	newNode := &FileNode{
		FileName:   filename,
		ParentNode: fileNode,
		ChildNodes: childNodes,
		Chunks:     chunks,
		Size:       size,
		IsFile:     isFile,
		mu:         &sync.RWMutex{},
	}
	fileNode.ChildNodes[filename] = newNode
	return nil, nil
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
	if newParentNode.ChildNodes[fileNode.FileName] != nil {
		return nil, fmt.Errorf("target path already has file with the same name, filename : %s", fileNode.FileName)
	}

	newParentNode.ChildNodes[fileNode.FileName] = fileNode
	delete(fileNode.ParentNode.ChildNodes, fileNode.FileName)
	fileNode.ParentNode = newParentNode
	return fileNode, nil
}

func (node FileNode) Remove(path string) (*FileNode, error) {
	fileNode, stack, isExist := getAndLockByPath(&node, path, false)
	defer unlockAllMutex(stack, false)
	if !isExist {
		return nil, fmt.Errorf("path not exist, path : %s", path)
	}

	delete(fileNode.ParentNode.ChildNodes, fileNode.FileName)
	fileNode.ParentNode = nil
	return fileNode, nil
}

func (node FileNode) List(path string) ([]*FileNode, error) {
	fileNode, stack, isExist := getAndLockByPath(&node, path, true)
	defer unlockAllMutex(stack, true)
	if !isExist {
		return nil, fmt.Errorf("path not exist, path : %s", path)
	}

	fileNodes := make([]*FileNode, len(fileNode.ChildNodes))
	return fileNodes, nil
}

func (node FileNode) Rename(path string, newName string) (*FileNode, error) {
	fileNode, stack, isExist := getAndLockByPath(&node, path, false)
	defer unlockAllMutex(stack, false)
	if !isExist {
		return nil, fmt.Errorf("path not exist, path : %s", path)
	}
	fileNode.FileName = newName
	return fileNode, nil
}

// 缓存池，最近用过的塞进去，超时就扔出去
