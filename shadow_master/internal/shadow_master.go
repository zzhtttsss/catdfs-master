package internal

import (
	"bufio"
	"container/list"
	"encoding/json"
	"github.com/sirupsen/logrus"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"tinydfs-base/protocol/pb"
	"tinydfs-base/util"
	"tinydfs-master/internal"
)

const (
	idIdx = iota
	fileNameIdx
	parentIdIdx
	childrenLengthIdx
	chunksIdIdx
	sizeIdx
	isFileIdx
	delTimeIdx
	isDelIdx
	lastLockTimeIdx
)

const (
	LogFileName       = ".\\edits.txt"
	DirectoryFileName = ".\\fsimage.txt"
)

type ShadowMaster struct {
	log        *logrus.Logger // FileWriter
	m          *sync.Mutex
	shadowRoot *internal.FileNode // The lazy copy of the directory in namespace
	flushTimer *time.Timer
}

func CreateShadowMaster() *ShadowMaster {
	SM := &ShadowMaster{
		log: logrus.New(),
		m:   &sync.Mutex{},
		shadowRoot: &internal.FileNode{
			Id:             util.GenerateUUIDString(),
			FileName:       "",
			ChildNodes:     make(map[string]*internal.FileNode),
			UpdateNodeLock: &sync.RWMutex{},
		},
	}
	SM.log.SetFormatter(&logrus.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
	})
	writer, err := os.OpenFile(LogFileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		log.Panicf("Create file %s failed: %v\n", LogFileName, err)
	}
	SM.log.SetOutput(writer)
	return SM
}

func (l *ShadowMaster) Info(a any) error {
	l.m.Lock()
	defer l.m.Unlock()
	data, err := json.Marshal(a)
	if err != nil {
		return err
	}
	l.log.Info(string(data))
	return nil
}

// TODO 感觉通过设计模式包装。读edit和读fsimage可以复用
// readLogLines read the `path` log and find all of the finished operations
func (l *ShadowMaster) readLogLines(path string) []*pb.OperationArgs {
	f, err := os.Open(path)
	if err != nil {
		log.Panicf("Open edits file failed.Error detail %v\n", err)
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
		// Map is used to filter which operation is finished
		_, ok := opMap[op.Uuid]
		if !ok {
			opMap[op.Uuid] = op
		} else {
			res = append(res, op)
		}
	}
	return res
}

func (l *ShadowMaster) readRootLines() map[string]*internal.FileNode {
	f, err := os.Open(internal.RootFileName)
	if err != nil {
		log.Panicf("Open fsimage file failed: %v\n", err)
		//TODO 需要额外处理
		return nil
	}
	buf := bufio.NewScanner(f)
	res := map[string]*internal.FileNode{}
	for buf.Scan() {
		line := buf.Text()
		data := strings.Split(line, " ")
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
		delTime, _ := time.Parse(internal.TimeFormat, data[delTimeIdx])
		var delTimePtr *time.Time
		if data[delTimeIdx] == "<nil>" {
			delTimePtr = nil
		} else {
			delTimePtr = &delTime
		}
		isDel, _ := strconv.ParseBool(data[isDelIdx])
		lastLockTime, _ := time.Parse(internal.TimeFormat, data[lastLockTimeIdx])
		fn := &internal.FileNode{
			Id:       data[idIdx],
			FileName: data[fileNameIdx],
			ParentNode: &internal.FileNode{
				Id: data[parentIdIdx],
			},
			ChildNodes:     map[string]*internal.FileNode{},
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
func (l *ShadowMaster) RootSerialize() {
	if l.shadowRoot == nil {
		return
	}
	// Write root level-order in the fsimage
	file, err := os.OpenFile(DirectoryFileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		logrus.Warnf("Open %s failed.Error Detail %s\n", DirectoryFileName, err)
		return
	}
	queue := list.New()
	queue.PushBack(l.shadowRoot)
	for queue.Len() != 0 {
		cur := queue.Back()
		queue.Remove(cur)
		node, ok := cur.Value.(*internal.FileNode)
		if !ok {
			logrus.Warnf("Element2FileNode failed\n")
		}
		_, err = file.WriteString(node.String())
		if err != nil {
			logrus.Warnf("Write String failed.Error Detail %s\n", err)
		}
		for _, child := range node.ChildNodes {
			queue.PushBack(child)
		}
	}
}

// RootDeserialize reads the fsimage.txt and rebuild the directory.
func (l *ShadowMaster) RootDeserialize(rootMap map[string]*internal.FileNode) {
	// Look for root
	for _, r := range rootMap {
		if r.ParentNode.Id == "-1" {
			l.shadowRoot = r
			break
		}
	}
	buildTree(l.shadowRoot, rootMap)
}

//TODO 时间复杂度n^n 可优化
func buildTree(parent *internal.FileNode, nodeMap map[string]*internal.FileNode) {
	for _, cur := range nodeMap {
		if cur.ParentNode.Id == parent.Id {
			parent.ChildNodes[cur.FileName] = cur
			buildTree(cur, nodeMap)
		}
	}
}
