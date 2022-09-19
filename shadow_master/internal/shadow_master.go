package internal

import (
	"encoding/json"
	"github.com/sirupsen/logrus"
	"log"
	"os"
	"sync"
	"time"
	"tinydfs-base/common"
	"tinydfs-base/util"
	"tinydfs-master/internal"
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
		TimestampFormat: common.NormalTimeFormat,
	})
	writer, err := os.OpenFile(common.LogFileName, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0755)
	if err != nil {
		log.Panicf("Create file %s failed: %v\n", common.LogFileName, err)
	}
	SM.writer = writer
	SM.log.SetOutput(writer)
	return SM
}

func (sm *ShadowMaster) Info(a any) error {
	sm.m.Lock()
	defer sm.m.Unlock()
	data, err := json.Marshal(a)
	if err != nil {
		return err
	}
	sm.log.Info(string(data))
	return nil
}
