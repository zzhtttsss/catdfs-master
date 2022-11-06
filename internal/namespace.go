package internal

import (
	"bufio"
	"container/list"
	"fmt"
	mapset "github.com/deckarep/golang-set"
	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
	"math"
	"sort"
	"strconv"
	"strings"
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
)

const (
	rootFileName     = ""
	pathSplitString  = "/"
	deleteFilePrefix = "delete_"
	deleteDelimiter  = "_"
)

var (
	// root is the root of the directory tree. The directory tree only exposes
	// root to the outside. All operations on the directory tree take root as
	// the entry. Access to this node must also be via the provided public methods
	// in this file.
	root = &FileNode{
		Id:         util.GenerateUUIDString(),
		FileName:   rootFileName,
		ChildNodes: make(map[string]*FileNode),
	}
	// fileNodeIdSet includes all fileNode id stored in the root.
	fileNodeIdSet = mapset.NewSet()
)

// FileNode represents a file or directory in the file system.
type FileNode struct {
	Id         string
	FileName   string
	ParentNode *FileNode
	// ChildNodes includes all child FileNode of this node, using FileName as key.
	ChildNodes map[string]*FileNode
	// Chunks is id of all Chunk in this file.
	Chunks []string
	// Size is the size of the file. Use bytes as the unit of measurement which
	// means 1kb will be 1024. The size of the directory is 0.
	Size   int64
	IsFile bool
	// DelTime represents the last time this FileNode was deleted. It is used to
	// determine whether this FileNode can be permanently deleted.
	DelTime *time.Time
	IsDel   bool
}

// CheckAndGetFileNode gets a FileNode by given path if the given path is legal.
func CheckAndGetFileNode(path string) (*FileNode, error) {
	fileNode, isExist := getFileNode(path)
	if !isExist {
		return nil, fmt.Errorf("path not exist, path : %s", path)
	}
	return fileNode, nil
}

// getAndLockByPath gets target FileNode.
func getFileNode(path string) (*FileNode, bool) {
	currentNode := root
	path = strings.Trim(path, pathSplitString)
	fileNames := strings.Split(path, pathSplitString)

	if path == root.FileName {
		return currentNode, true
	}

	for _, name := range fileNames {
		nextNode, exist := currentNode.ChildNodes[name]
		if !exist {
			return nil, false
		}
		currentNode = nextNode
	}
	return currentNode, true
}

// AddFileNode add a FileNode to directory tree. It is generally used to add a
// directory because it will unlock all FileNode after adding the FileNode to
// directory tree.
func AddFileNode(path string, filename string, size int64, isFile bool) (*FileNode, error) {
	fileNode, isExist := getFileNode(path)
	if !isExist || fileNode.IsFile {
		return nil, fmt.Errorf("path not exist, path : %s", path)
	}

	if _, ok := fileNode.ChildNodes[filename]; ok {
		return nil, fmt.Errorf("target path already has file with the same name, path : %s", path)
	}

	id := util.GenerateUUIDString()
	newNode := &FileNode{
		Id:         id,
		FileName:   filename,
		ParentNode: fileNode,
		Size:       size,
		IsFile:     isFile,
		IsDel:      false,
		DelTime:    nil,
	}
	if isFile {
		fileNodeIdSet.Add(newNode.Id)
		fmt.Printf("1 fileNodeIdSet add: %s", fileNodeIdSet.String())
		newNode.Chunks = initChunks(size, id)

	} else {
		newNode.ChildNodes = make(map[string]*FileNode)
	}
	fileNode.ChildNodes[filename] = newNode
	return newNode, nil
}

// LockAndAddFileNode add a FileNode to directory tree without unlock any FileNode.
// It is generally used to add a file. After calling this method, we must call
// UnlockFileNodesById to unlock all FileNode.
func LockAndAddFileNode(id string, path string, filename string, size int64, isFile bool) (*FileNode, error) {
	fileNode, isExist := getFileNode(path)
	if !isExist || fileNode.IsFile {
		return nil, fmt.Errorf("path not exist, path : %s", path)
	}
	if _, ok := fileNode.ChildNodes[filename]; ok {
		return nil, fmt.Errorf("target path already has file with the same name, path : %s", path)
	}
	newNode := &FileNode{
		Id:         id,
		FileName:   filename,
		ParentNode: fileNode,
		Size:       size,
		IsFile:     isFile,
		IsDel:      false,
		DelTime:    nil,
	}
	if isFile {
		fileNodeIdSet.Add(newNode.Id)
		newNode.Chunks = initChunks(size, id)
	} else {
		newNode.ChildNodes = make(map[string]*FileNode)
	}
	fileNode.ChildNodes[filename] = newNode
	return newNode, nil
}

func initChunks(size int64, id string) []string {
	nums := int(math.Ceil(float64(size) / float64(common.ChunkSize)))
	chunks := make([]string, nums)
	for i := 0; i < len(chunks); i++ {
		chunks[i] = id + strconv.Itoa(i)
	}
	return chunks
}

// MoveFileNode move a FileNode to target path.
func MoveFileNode(currentPath string, targetPath string) (*FileNode, error) {
	fileNode, isExist := getFileNode(currentPath)
	newParentNode, isParentExist := getFileNode(targetPath)
	if !isExist {
		return nil, fmt.Errorf("current path not exist, path : %s", currentPath)
	}
	if !isParentExist {
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

// RemoveFileNode remove a FileNode from file system. It should be noted that
// this method is dummy delete, and does not actually remove the FileNode from
// the directory tree. This method will prefix the node's name with "delete"
// and update isDel and delTime of the FileNode. The delete state will be
// canceled when the user renames the FileNode within a certain period of time.
func RemoveFileNode(path string) (*FileNode, error) {
	return removeFileNode(path, true)
}

// EraseFileNode will completely delete a FileNode. It is only used internally.
// It will set the delTime of a FileNode to one year ago, so the monitor goroutine
// will completely delete the FileNode
func EraseFileNode(path string) (*FileNode, error) {
	return removeFileNode(path, false)
}

func removeFileNode(path string, isDummy bool) (*FileNode, error) {
	fileNode, isExist := getFileNode(path)
	if !isExist {
		return nil, fmt.Errorf("path not exist, path : %s", path)
	}

	delete(fileNode.ParentNode.ChildNodes, fileNode.FileName)
	fileNode.FileName = deleteFilePrefix + fileNode.Id + deleteDelimiter + fileNode.FileName
	fileNode.ParentNode.ChildNodes[fileNode.FileName] = fileNode

	fileNode.IsDel = true
	delTime := time.Now()
	if !isDummy {
		delTime = delTime.AddDate(-1, 0, 0)
	}
	fileNode.DelTime = &(delTime)
	return fileNode, nil
}

// ListFileNode get a slice including all FileNode under the specified path.
// The path must be a directory not a file.
func ListFileNode(path string) ([]*FileNode, error) {
	fileNode, isExist := getFileNode(path)
	if !isExist || fileNode.IsFile {
		return nil, fmt.Errorf("path not exist, path : %s", path)
	}

	fileNodes := make([]*FileNode, len(fileNode.ChildNodes))
	nodeIndex := 0
	for _, n := range fileNode.ChildNodes {
		fileNodes[nodeIndex] = n
		nodeIndex++
	}
	return fileNodes, nil
}

// RenameFileNode rename a FileNode to given name.
func RenameFileNode(path string, newName string) (*FileNode, error) {
	fileNode, isExist := getFileNode(path)
	if !isExist {
		return nil, fmt.Errorf("path not exist, path : %s", path)
	}

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
		res.WriteString(fmt.Sprintf("%s$%s$%s$%v$%s$%d$%v$%v$%v\n",
			f.Id, f.FileName, common.MinusOneString, childrenIds, f.Chunks,
			f.Size, f.IsFile, f.DelTime, f.IsDel))
	} else {
		res.WriteString(fmt.Sprintf("%s$%s$%s$%v$%s$%d$%v$%v$%v\n",
			f.Id, f.FileName, f.ParentNode.Id, childrenIds, f.Chunks,
			f.Size, f.IsFile, f.DelTime, f.IsDel))

	}

	return res.String()
}

// IsDeepEqualTo is used to compare two FileNode.
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

// PersistDirTree writes all FileNode in the directory tree to the sink for
// persistence.
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

// RestoreDirTree restore the directory tree from buf.
func RestoreDirTree(buf *bufio.Scanner) error {
	logrus.Println("Start to restore directory tree.")
	rootMap := ReadDirTree(buf)
	if rootMap != nil && len(rootMap) != 0 {
		logrus.Println("Start to Deserialize directory tree.")
		root = RootDeserialize(rootMap)
	}
	return nil
}

// ReadDirTree reads all FileNode from the buf and puts them into a map.
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
		if isDel && time.Now().Sub(delTime).Hours() > 23 {
			continue
		}
		fn := &FileNode{
			Id:       data[FileNodeIdIdx],
			FileName: data[fileNameIdx],
			ParentNode: &FileNode{
				Id: data[parentIdIdx],
			},
			ChildNodes: children,
			Chunks:     chunks,
			Size:       int64(size),
			IsFile:     isFile,
			DelTime:    delTimePtr,
			IsDel:      isDel,
		}
		res[fn.Id] = fn
	}
	return res
}

// RootDeserialize rebuild the directory tree from rootMap.
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
		if node.IsFile {
			fileNodeIdSet.Add(node.Id)
		}
		buildTree(node, nodeMap)
	}
}
