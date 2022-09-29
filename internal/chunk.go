package internal

import "sync"

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
