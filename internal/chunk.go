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
	// chunksMap stores all Chunk in the file system, using id as the key.
	chunksMap        = make(map[string]*Chunk)
	updateChunksLock = &sync.RWMutex{}
)

type Chunk struct {
	// Id is FileNodeId+_+ChunkNum
	Id string
	// dataNodes includes all id of DataNode which are storing this Chunk.
	dataNodes []string
	// primaryNode is the id of DataNode which has the lease of this Chunk.
	// Operations involving the chunkserver are all communicated with the
	// client by this DataNode
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

// PersistChunks writes all Chunk in chunksMap to the sink for persistence.
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

// RestoreChunks reads all Chunk from the buf and puts them into chunksMap.
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
