package internal

import (
	"bufio"
	"container/list"
	"encoding/json"
	"github.com/hashicorp/raft"
	"github.com/sirupsen/logrus"
	"io"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"tinydfs-base/common"
)

type ApplyResponse struct {
	Response interface{}
	Error    error
}

// MasterFSM implement FSM and make use of the replicated log.
type MasterFSM struct {
}

// Apply call Apply function of operation, changes to metadata will be made in that function.
func (ms MasterFSM) Apply(l *raft.Log) interface{} {
	operation := ConvBytes2Operation(l.Data)
	response, err := operation.Apply()
	return &ApplyResponse{
		Response: response,
		Error:    err,
	}
}

// ConvBytes2Operation Use reflect to restore operation from data
func ConvBytes2Operation(data []byte) Operation {
	opContainer := OpContainer{}
	var operation Operation
	err := json.Unmarshal(data, &opContainer)
	if err != nil {
		return nil
	}
	operation = reflect.New(OpTypeMap[opContainer.OpType]).Interface().(Operation)
	err = json.Unmarshal(opContainer.OpData, operation)
	if err != nil {
		return nil
	}
	return operation
}

func (ms MasterFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &snapshot{}, nil
}

func (ms MasterFSM) Restore(r io.ReadCloser) error {
	logrus.Println("Start to restore metadata.")
	rootMap := ReadSnapshotLines(r)
	if rootMap != nil && len(rootMap) != 0 {
		logrus.Println("Start to Deserialize directory tree.")
		root = RootDeserialize(rootMap)
	}
	return r.Close()
}

type snapshot struct {
}

// Persist Take a snapshot of current metadata.
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
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
			logrus.Errorf("Fail to write string, error detail %s", err.Error())
		}
		for _, child := range node.ChildNodes {
			queue.PushBack(child)
		}
	}
	return sink.Close()
}

func (s *snapshot) Release() {

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
