package internal

import (
	"bufio"
	"container/list"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"tinydfs-base/common"
	"tinydfs-base/util"
)

const (
	FileNodeIdIdx = iota
	fileNameIdx
	parentIdIdx
	childrenIdx
	fileChunksIdx
	sizeIdx
	isFileIdx
	delTimeIdx
	isDelIdx
	lastLockTimeIdx
)

const (
	rootFileName     = ""
	pathSplitString  = "/"
	deleteFilePrefix = "delete"
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
	lockedFileNodes  = make(map[string]*list.List)
	fileNodesMapLock = &sync.Mutex{}
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

func CheckAndGetFileNode(path string) (*FileNode, error) {
	fileNode, stack, isExist := getAndLockByPath(path, true)
	if !isExist {
		return nil, fmt.Errorf("path not exist, path : %s", path)
	}
	defer unlockAllMutex(stack, true)
	return fileNode, nil
}

func getAndLockByPath(path string, isRead bool) (*FileNode, *list.List, bool) {
	currentNode := root
	path = strings.Trim(path, pathSplitString)
	fileNames := strings.Split(path, pathSplitString)
	stack := list.New()

	if path == root.FileName {
		if isRead {
			currentNode.UpdateNodeLock.RLock()
		} else {
			currentNode.UpdateNodeLock.Lock()
		}
		currentNode.LastLockTime = time.Now()
		stack.PushBack(currentNode)
		return currentNode, stack, true
	}

	for _, name := range fileNames {
		currentNode.UpdateNodeLock.RLock()
		currentNode.LastLockTime = time.Now()
		stack.PushBack(currentNode)
		nextNode, exist := currentNode.ChildNodes[name]
		if !exist {
			unlockAllMutex(stack, true)
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

func unlockAllMutex(stack *list.List, isRead bool) {
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

func UnlockFileNodesById(fileNodeId string, isRead bool) error {
	fileNodesMapLock.Lock()
	stack, ok := lockedFileNodes[fileNodeId]
	fileNodesMapLock.Unlock()
	if !ok {
		logrus.Errorf("fail to find stack by FileNodeId : %s", fileNodeId)
		return fmt.Errorf("fail to find stack by FileNodeId : %s", fileNodeId)
	}
	unlockAllMutex(stack, isRead)
	return nil
}

func AddFileNode(path string, filename string, size int64, isFile bool) (*FileNode, error) {
	fileNode, stack, isExist := getAndLockByPath(path, false)
	if !isExist || fileNode.IsFile {
		return nil, fmt.Errorf("path not exist, path : %s", path)
	}
	defer unlockAllMutex(stack, false)

	if _, ok := fileNode.ChildNodes[filename]; ok {
		return nil, fmt.Errorf("target path already has file with the same name, path : %s", path)
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
	return newNode, nil
}

func LockAndAddFileNode(id string, path string, filename string, size int64, isFile bool) (*FileNode, *list.List, error) {
	fileNode, stack, isExist := getAndLockByPath(path, false)
	if !isExist || fileNode.IsFile {
		return nil, nil, fmt.Errorf("path not exist, path : %s", path)
	}
	if _, ok := fileNode.ChildNodes[filename]; ok {
		unlockAllMutex(stack, false)
		return nil, nil, fmt.Errorf("target path already has file with the same name, path : %s", path)
	}
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
	nums := int(math.Ceil(float64(size) / float64(common.ChunkSize)))
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
	defer unlockAllMutex(stack, false)
	if !isParentExist {
		return nil, fmt.Errorf("target path not exist, path : %s", targetPath)
	}
	defer unlockAllMutex(parentStack, false)
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
	defer unlockAllMutex(stack, false)

	delete(fileNode.ParentNode.ChildNodes, fileNode.FileName)
	fileNode.FileName = deleteFilePrefix + fileNode.FileName
	fileNode.ParentNode.ChildNodes[fileNode.FileName] = fileNode

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
	defer unlockAllMutex(stack, true)

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
	defer unlockAllMutex(stack, false)

	delete(fileNode.ParentNode.ChildNodes, fileNode.FileName)
	fileNode.FileName = newName
	fileNode.ParentNode.ChildNodes[fileNode.FileName] = fileNode
	if fileNode.IsDel {
		fileNode.IsDel = false
		fileNode.DelTime = nil
	}
	return fileNode, nil
}

func StatFileNode(path string) (*FileNode, error) {
	return CheckAndGetFileNode(path)
}

func (f *FileNode) String() string {
	res := strings.Builder{}
	childrenIds := make([]string, 0)
	for _, n := range f.ChildNodes {
		childrenIds = append(childrenIds, n.Id)
	}
	if f.ParentNode == nil {
		res.WriteString(fmt.Sprintf("%s$%s$%s$%v$%s$%d$%v$%v$%v$%s\n",
			f.Id, f.FileName, common.MinusOneString, childrenIds, f.Chunks,
			f.Size, f.IsFile, f.DelTime, f.IsDel, f.LastLockTime.Format(common.LogFileTimeFormat)))
	} else {
		res.WriteString(fmt.Sprintf("%s$%s$%s$%v$%s$%d$%v$%v$%v$%s\n",
			f.Id, f.FileName, f.ParentNode.Id, childrenIds, f.Chunks,
			f.Size, f.IsFile, f.DelTime, f.IsDel, f.LastLockTime.Format(common.LogFileTimeFormat)))

	}

	return res.String()
}

// IsDeepEqualTo is used to compare two FileNode
func (f *FileNode) IsDeepEqualTo(t *FileNode) bool {
	arr1 := make([]*FileNode, 0)
	arr2 := make([]*FileNode, 0)
	f.add2Arr(&arr1)
	t.add2Arr(&arr2)
	if len(arr1) != len(arr2) {
		return false
	}
	for i := 0; i < len(arr1); i++ {
		s1 := arr1[i].String()
		s2 := arr2[i].String()
		if s1 != s2 {
			return false
		}
	}
	return true
}

func (f *FileNode) add2Arr(arr *[]*FileNode) {
	if f == nil {
		return
	}
	*arr = append(*arr, f)
	// Guaranteed iteration order
	children := make([]string, len(f.ChildNodes))
	for key, _ := range f.ChildNodes {
		children = append(children, key)
	}
	sort.Strings(children)
	for _, child := range children {
		f.ChildNodes[child].add2Arr(arr)
	}
}

func PersistDirTree(sink raft.SnapshotSink) error {
	queue := list.New()
	queue.PushBack(root)
	for queue.Len() != 0 {
		cur := queue.Front()
		queue.Remove(cur)
		node, ok := cur.Value.(*FileNode)
		if !ok {
			logrus.Warnf("Fail to convert element to FileNode")
		}
		_, err := sink.Write([]byte(node.String()))
		if err != nil {
			return err
		}
		for _, child := range node.ChildNodes {
			queue.PushBack(child)
		}
	}
	_, err := sink.Write([]byte(common.SnapshotDelimiter))
	if err != nil {
		return err
	}
	return nil
}

func RestoreDirTree(buf *bufio.Scanner) error {
	logrus.Println("Start to restore directory tree.")
	rootMap := ReadDirTree(buf)
	if rootMap != nil && len(rootMap) != 0 {
		logrus.Println("Start to Deserialize directory tree.")
		root = RootDeserialize(rootMap)
	}
	return nil
}

// ReadDirTree returns map whose key is the id of the FileNode rather than filename
func ReadDirTree(buf *bufio.Scanner) map[string]*FileNode {
	res := map[string]*FileNode{}
	for buf.Scan() {
		line := buf.Text()
		if line == common.SnapshotDelimiter {
			return res
		}
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
		chunksLen := len(data[fileChunksIdx])
		chunksData := data[fileChunksIdx][1 : chunksLen-1]
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
			Id:       data[FileNodeIdIdx],
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

// RootDeserialize reads the snapshot and rebuild the directory.
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

// buildTree build the directory tree from given map containing all FileNode.
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
