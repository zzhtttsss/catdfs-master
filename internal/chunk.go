package internal

import (
	"bufio"
	"fmt"
	"github.com/hashicorp/raft"
	"strings"
	"sync"
	"tinydfs-base/common"
)

const (
	chunkIdIdx = iota
	dataNodesIdx
	primaryNodeIdx
)

var (
	// Store all Chunk, using id as the key
	chunksMap        = make(map[string]*Chunk)
	updateChunksLock = &sync.RWMutex{}
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
	chunksMap[chunk.Id] = chunk
	updateChunksLock.Unlock()
}

func GetChunk(id string) *Chunk {
	updateChunksLock.RLock()
	defer func() {
		updateChunksLock.RUnlock()
	}()
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
