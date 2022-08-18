package model

import (
	"container/list"
	"fmt"
	"strings"
	"sync"
)

const (
	mb           = 1048576
	chunkSize    = 64
	rootFileName = ""
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
	fileNode, stack, isExist := checkAndGetByPath(&node, path, true)
	defer unlockAllMutex(stack, true)
	if !isExist {
		return nil, fmt.Errorf("path not exist, path : %s", path)
	}
	return fileNode, nil
}

func checkAndGetByPath(node *FileNode, path string, isRead bool) (*FileNode, *list.List, bool) {
	currentNode := node
	fileNames := strings.Split(path, "/")
	stack := list.New()

	for _, name := range fileNames {
		if isRead {
			currentNode.mu.RLock()
		} else {
			currentNode.mu.Lock()
		}
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
	for stack.Len() != 0 {
		element := stack.Back()
		node := element.Value.(FileNode)
		if isRead {
			node.mu.RUnlock()
		} else {
			node.mu.Unlock()
		}
		stack.Remove(element)
	}
}

func (node FileNode) Add(path string, filename string, size int, isFile bool) (*FileNode, error) {
	var (
		chunks     []string
		childNodes map[string]*FileNode
	)

	fileNode, stack, isExist := checkAndGetByPath(&node, path, false)
	defer unlockAllMutex(stack, false)
	if !isExist {
		return nil, fmt.Errorf("path not exist, path : %s", path)
	}
	if isFile {
		chunks = make([]string, size/(chunkSize*mb))
	} else {
		childNodes = make(map[string]*FileNode)
	}

	newNode := &FileNode{
		FileName:   filename,
		ParentNode: fileNode,
		ChildNodes: childNodes,
		Chunks:     chunks,
		IsFile:     isFile,
		mu:         &sync.RWMutex{},
	}
	fileNode.ChildNodes[filename] = newNode
	return nil, nil
}

func (node FileNode) Move(currentPath string, targetPath string) (*FileNode, error) {
	fileNode, stack, isExist := checkAndGetByPath(&node, currentPath, false)
	defer unlockAllMutex(stack, false)
	newParentNode, parentStack, isParentExist := checkAndGetByPath(&node, targetPath, false)
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
	fileNode, stack, isExist := checkAndGetByPath(&node, path, false)
	defer unlockAllMutex(stack, false)
	if !isExist {
		return nil, fmt.Errorf("path not exist, path : %s", path)
	}

	delete(fileNode.ParentNode.ChildNodes, fileNode.FileName)
	fileNode.ParentNode = nil
	return fileNode, nil
}

func (node FileNode) list(path string) ([]*FileNode, error) {
	fileNode, stack, isExist := checkAndGetByPath(&node, path, true)
	defer unlockAllMutex(stack, true)
	if !isExist {
		return nil, fmt.Errorf("path not exist, path : %s", path)
	}

	fileNodes := make([]*FileNode, len(fileNode.ChildNodes))
	return fileNodes, nil
}

// 缓存池，最近用过的塞进去，超时就扔出去
