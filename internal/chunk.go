package internal

import (
	"bufio"
	"context"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/spf13/viper"
	"go.uber.org/atomic"
	"strings"
	"sync"
	"time"
	"tinydfs-base/common"
	"tinydfs-base/util"
)

const (
	chunkIdIdx = iota
	dataNodesIdx
	primaryNodeIdx
	Processing
	Waiting
)

var (
	// Store all Chunk, using id as the key
	chunksMap        = make(map[string]*Chunk)
	updateChunksLock = &sync.RWMutex{}
	// Store all dead chunks that are waiting to be copied
	deadChunk = &DeadChunkQueue{
		queue: util.NewQueue[String](),
		state: atomic.NewUint32(Waiting),
	}
)

type Chunk struct {
	// ChunkId = FileNodeId+_+ChunkNum
	Id string
	// id of all DataNode storing this Chunk.
	dataNodes   []string
	primaryNode string
}

func (c *Chunk) String() string {
	res := strings.Builder{}
	dataNodes := make([]string, len(c.dataNodes))
	for i, dataNodeId := range c.dataNodes {
		dataNodes[i] = dataNodeId
	}

	res.WriteString(fmt.Sprintf("%s$%v$%s\n",
		c.Id, dataNodes, c.primaryNode))
	return res.String()
}

func AddChunk(chunk *Chunk) {
	updateChunksLock.Lock()
	defer updateChunksLock.Unlock()
	chunksMap[chunk.Id] = chunk
}

func GetChunk(id string) *Chunk {
	updateChunksLock.RLock()
	defer updateChunksLock.RUnlock()
	return chunksMap[id]
}

func PersistChunks(sink raft.SnapshotSink) error {
	for _, chunk := range chunksMap {
		_, err := sink.Write([]byte(chunk.String()))
		if err != nil {
			return err
		}
	}
	_, err := sink.Write([]byte(common.SnapshotDelimiter))
	if err != nil {
		return err
	}
	return nil
}

func RestoreChunks(buf *bufio.Scanner) error {
	chunksMap = map[string]*Chunk{}
	for buf.Scan() {
		line := buf.Text()
		if line == common.SnapshotDelimiter {
			return nil
		}
		data := strings.Split(line, "$")

		dataNodesLen := len(data[dataNodesIdx])
		chunksData := data[dataNodesIdx][1 : dataNodesLen-1]
		dataNodes := strings.Split(chunksData, " ")
		chunksMap[data[chunkIdIdx]] = &Chunk{
			Id:          data[chunkIdIdx],
			dataNodes:   dataNodes,
			primaryNode: data[primaryNodeIdx],
		}
	}
	return nil
}

type String string

func (s String) String() string {
	return string(s)
}

type DeadChunkQueue struct {
	queue *util.Queue[String]
	state *atomic.Uint32
}

func (q *DeadChunkQueue) String() string {
	return q.queue.String()
}

func PersistDeadChunkQueue(sink raft.SnapshotSink) error {
	_, err := sink.Write([]byte(deadChunk.String()))
	if err != nil {
		return err
	}

	_, err = sink.Write([]byte(common.SnapshotDelimiter))
	if err != nil {
		return err
	}
	return nil
}

func RestoreDeadChunkQueue(buf *bufio.Scanner) error {
	for buf.Scan() {
		line := buf.Text()
		if line == common.SnapshotDelimiter {
			return nil
		}
		line = strings.Trim(line, common.DollarDelimiter)
		data := strings.Split(line, "$")
		for _, datum := range data {
			deadChunk.queue.Push(String(datum))
		}
	}
	return nil
}

// MonitorDeadChunk Run in a goroutine.
// This function will monitor dead chunk queue.
// Start copy chunks when timer elapse or the number of dead chunks equals to #{ChunkDeadChunkCopyThreshold}
func MonitorDeadChunk(ctx context.Context) {
	subContext, subCancel := context.WithCancel(ctx)
	if deadChunk.queue.Len() > 0 && deadChunk.state.CAS(Waiting, Processing) {
		go DuplicateDeadChunk(subContext)
	}
	timer := time.NewTicker(time.Duration(viper.GetInt(common.ChunkDeadChunkCheckTime)) * time.Second)
	for {
		select {
		case <-timer.C:
			if deadChunk.state.CAS(Waiting, Processing) {
				go DuplicateDeadChunk(subContext)
			}
		case <-ctx.Done():
			timer.Stop()
			subCancel()
			return
		default:
			if deadChunk.queue.Len() >= viper.GetInt(common.ChunkDeadChunkCopyThreshold) {
				if deadChunk.state.CAS(Waiting, Processing) {
					go DuplicateDeadChunk(subContext)
				}
			}
			time.Sleep(time.Second)
		}
	}
}

// DuplicateDeadChunk Runs in a goroutine.
// This function is to copy chunks in dead chunk queue whose steps are:
//1. Get all dataNodes that store the specified dead chunk,
//2. Select one dataNode as fromDataNode which has the lease number of chunk or leases,
//3. Select one dataNode as toDataNode which does not store the chunk in others.
// (写给自己) 如果在这个过程进行当中，leader宕机，那么：
// 问题1. 元数据的更新并不会commit到各个follower和新leader上。所以新leader并不知道这些deadChunk哪些存了，哪些没存。
// 解决办法：新leader检查deadChunk的queue数量，如果里面有，直接进行保存。(后果可能会造成某些chunk多存储一份)
// 问题2. 由于先进行复制，后写log，所以会造成log没有写完，但chunk已经复制完毕的情况。因此，活着的dataNode的chunkMap
// 是和机器上实际情况不匹配的。但新leader的copy过程执行完毕且apply之后，会解决这个问题。
func DuplicateDeadChunk(ctx context.Context) {
	var (
		maxCount  = viper.GetInt(common.ChunkDeadChunkCopyThreshold)
		copyCount int
	)
	if deadChunk.queue.Len() > maxCount {
		copyCount = maxCount
	} else {
		copyCount = deadChunk.queue.Len()
	}
	//TODO
	fmt.Println(copyCount)

}
