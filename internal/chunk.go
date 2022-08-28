package internal

import "sync"

var (
	chunksMap        = make(map[string]*Chunk)
	updateChunksLock = &sync.RWMutex{}
)

type Chunk struct {
	Id          string
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
