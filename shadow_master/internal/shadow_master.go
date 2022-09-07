package internal

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/sirupsen/logrus"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"tinydfs-base/util"
	"tinydfs-master/internal"
)

const (
	LogFileName = ".\\edits.txt"
	idIdx       = iota - 1
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

type ShadowMaster struct {
	log        *logrus.Logger // FileWriter
	m          *sync.Mutex
	shadowRoot *internal.FileNode // The lazy copy of root in namespace
}

var SM *ShadowMaster

func init() {
	SM = &ShadowMaster{
		log: logrus.New(),
		m:   &sync.Mutex{},
	}
	SM.log.SetFormatter(&logrus.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
	})
	writer, err := os.OpenFile(LogFileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		log.Panicf("Create file %s failed: %v\n", LogFileName, err)
	}
	SM.log.SetOutput(writer)
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

//TODO 感觉通过设计模式包装。读edit和读fsimage可以复用
func (l *ShadowMaster) readLogLines() []*internal.Operation {
	f, err := os.Open(LogFileName)
	if err != nil {
		log.Panicf("Open edits file failed: %v\n", err)
		//TODO 需要额外处理
		return nil
	}
	buf := bufio.NewScanner(f)
	res := make([]*internal.Operation, 0)
	for buf.Scan() {
		line := buf.Text()
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		line = strings.ReplaceAll(line, "\\", "")
		left := strings.Index(line, "{")
		right := strings.Index(line, "}")
		op := &internal.Operation{}
		fmt.Println(line)
		util.Unmarshal(line[left:right+1], op)
		res = append(res, op)
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

func (l *ShadowMaster) RootUnSerialize(rootMap map[string]*internal.FileNode) {
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

// MergeRootTree merges the operations onto shadow root
func (l *ShadowMaster) MergeRootTree() {
	if l.shadowRoot == nil {
		logrus.Panicf("Should Unserialize fsimage first!")
		return
	}
	ops := l.readLogLines()
	for _, op := range ops {
		switch op.Type {
		case internal.Operation_Add:
			l.add(op.Des, op.IsFile)
		case internal.Operation_Remove:
			l.remove(op.Des)
		case internal.Operation_Rename:
			l.rename(op.Src, op.Des)
		case internal.Operation_Move:
			l.move(op.Src, op.Des)
		default:
			logrus.Panicf("Error Operation Type.")
		}
	}
	_ = os.Remove(LogFileName)
}

func (l *ShadowMaster) add(des string, file bool) {

}

func (l *ShadowMaster) remove(des string) {

}

func (l *ShadowMaster) rename(src string, des string) {

}

func (l *ShadowMaster) move(src string, des string) {

}
