package internal

import (
	"fmt"
	mapset "github.com/deckarep/golang-set"
	"testing"
	"time"
	"tinydfs-base/common"
)

func TestMain(m *testing.M) {
	dataNodeMap = map[string]*DataNode{
		"dn1": {
			Id:            "dn1",
			status:        common.Alive,
			Address:       "dn1:6789",
			Chunks:        mapset.NewSetWith("chunk1", "chunk2", "chunk3"),
			Leases:        mapset.NewSetWith("chunk1"),
			HeartbeatTime: time.Time{},
		},
		"dn2": {
			Id:            "dn2",
			status:        common.Alive,
			Address:       "dn2:6789",
			Chunks:        mapset.NewSetWith("chunk1", "chunk2", "chunk3", "chunk4", "chunk5", "chunk6"),
			Leases:        mapset.NewSet(),
			HeartbeatTime: time.Time{},
		},
		"dn3": {
			Id:            "dn3",
			status:        common.Alive,
			Address:       "dn3:6789",
			Chunks:        mapset.NewSetWith("chunk1", "chunk2", "chunk4"),
			Leases:        mapset.NewSetWith("chunk4"),
			HeartbeatTime: time.Time{},
		},
		"dn4": {
			Id:            "dn4",
			status:        common.Alive,
			Address:       "dn4:6789",
			Chunks:        mapset.NewSetWith("chunk3", "chunk5", "chunk6"),
			Leases:        mapset.NewSet(),
			HeartbeatTime: time.Time{},
		},
		"dn5": {
			Id:            "dn5",
			status:        common.Alive,
			Address:       "dn5:6789",
			Chunks:        mapset.NewSetWith("chunk4", "chunk6"),
			Leases:        mapset.NewSet(),
			HeartbeatTime: time.Time{},
		},
	}
	m.Run()
}

type stu struct {
	a mapset.Set
	b map[ChunkSendInfo]int
}

func TestGetDataNode2MoveChunk(t *testing.T) {
	mm := map[*stu]int{}
	aa := &stu{
		a: mapset.NewSet("aa"),
		b: map[ChunkSendInfo]int{
			ChunkSendInfo{
				ChunkId:    "sdf",
				DataNodeId: "adsf",
				SendType:   0,
			}: 2,
		},
	}
	mm[aa] = 1
	mm[aa] = 2
	fmt.Println(mm[aa])
}
