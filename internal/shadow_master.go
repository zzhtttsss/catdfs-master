package internal

import (
	"bufio"
	"fmt"
	"github.com/sirupsen/logrus"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"tinydfs-base/util"
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

type Logger struct {
	log        *logrus.Logger // FileWriter
	m          *sync.Mutex
	shadowRoot *FileNode // The lazy copy of root in namespace
}

var SM *Logger

func init() {
	SM = &Logger{
		log: logrus.New(),
		m:   &sync.Mutex{},
	}
	SM.log.SetFormatter(&logrus.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05",
	})
	writer1 := os.Stdout
	writer2, err := os.OpenFile(LogFileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755)
	if err != nil {
		log.Panicf("Create file %s failed: %v\n", LogFileName, err)
	}
	SM.log.SetOutput(io.MultiWriter(writer1, writer2))
}

func (l *Logger) Info(a any) {
	l.m.Lock()
	defer l.m.Unlock()
	l.log.Info(util.Marshal(a))
}

//TODO 感觉通过设计模式包装。读edit和读fsimage可以复用
func (l *Logger) readLogLines() []*Operation {
	f, err := os.Open(LogFileName)
	if err != nil {
		log.Panicf("Open edits file failed: %v\n", err)
		//TODO 需要额外处理
		return nil
	}
	buf := bufio.NewScanner(f)
	res := make([]*Operation, 0)
	for buf.Scan() {
		line := buf.Text()
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		line = strings.ReplaceAll(line, "\\", "")
		left := strings.Index(line, "{")
		right := strings.Index(line, "}")
		op := &Operation{}
		fmt.Println(line)
		util.Unmarshal(line[left:right+1], op)
		res = append(res, op)
	}
	return res
}

func (l *Logger) readRootLines() map[string]*FileNode {
	f, err := os.Open(RootFileName)
	if err != nil {
		log.Panicf("Open fsimage file failed: %v\n", err)
		//TODO 需要额外处理
		return nil
	}
	buf := bufio.NewScanner(f)
	res := map[string]*FileNode{}
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
		delTime, _ := time.Parse(TimeFormat, data[delTimeIdx])
		var delTimePtr *time.Time
		if data[delTimeIdx] == "<nil>" {
			delTimePtr = nil
		} else {
			delTimePtr = &delTime
		}
		isDel, _ := strconv.ParseBool(data[isDelIdx])
		lastLockTime, _ := time.Parse(TimeFormat, data[lastLockTimeIdx])
		fn := &FileNode{
			Id:       data[idIdx],
			FileName: data[fileNameIdx],
			parentNode: &FileNode{
				Id: data[parentIdIdx],
			},
			childNodes:     map[string]*FileNode{},
			Chunks:         chunks,
			Size:           int64(size),
			IsFile:         isFile,
			DelTime:        delTimePtr,
			IsDel:          isDel,
			updateNodeLock: &sync.RWMutex{},
			LastLockTime:   lastLockTime,
		}
		res[fn.Id] = fn
	}
	return res
}

func (l *Logger) RootUnSerialize(rootMap map[string]*FileNode) {
	// Look for root
	for _, r := range rootMap {
		if r.parentNode.Id == "-1" {
			l.shadowRoot = r
			break
		}
	}
	buildTree(l.shadowRoot, rootMap)
}

//TODO 时间复杂度n^n 可优化
func buildTree(parent *FileNode, nodeMap map[string]*FileNode) {
	for _, cur := range nodeMap {
		if cur.parentNode.Id == parent.Id {
			parent.childNodes[cur.FileName] = cur
			buildTree(cur, nodeMap)
		}
	}
}

// MergeRootTree merges the operations onto shadow root
func (l *Logger) MergeRootTree() {
	if l.shadowRoot == nil {
		logrus.Panicf("Should Unserialize fsimage first!")
		return
	}
	ops := l.readLogLines()
	for _, op := range ops {
		switch op.Type {
		case Operation_Add:
			l.add(op.Des, op.IsFile)
		case Operation_Remove:
			l.remove(op.Des)
		case Operation_Rename:
			l.rename(op.Src, op.Des)
		case Operation_Move:
			l.move(op.Src, op.Des)
		default:
			logrus.Panicf("Error Operation Type.")
		}
	}
	_ = os.Remove(LogFileName)
}

func (l *Logger) add(des string, file bool) {

}

func (l *Logger) remove(des string) {

}

func (l *Logger) rename(src string, des string) {

}

func (l *Logger) move(src string, des string) {

}
