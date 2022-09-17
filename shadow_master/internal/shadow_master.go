package internal

import (
	"encoding/json"
	"github.com/sirupsen/logrus"
	"log"
	"os"
	"sync"
	"time"
	"tinydfs-base/util"
	"tinydfs-master/internal"
)

const (
	LogFileName       = "log/edits.txt"
	DirectoryFileName = "log/fsimage.txt"
)

type ShadowMaster struct {
	log        *logrus.Logger // FileWriter
	m          *sync.Mutex
	shadowRoot *internal.FileNode // The lazy copy of the directory in namespace
	flushTimer *time.Timer
	writer     *os.File
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
	writer, err := os.OpenFile(LogFileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0755)
	if err != nil {
		log.Panicf("Create file %s failed: %v\n", LogFileName, err)
	}
	SM.writer = writer
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
