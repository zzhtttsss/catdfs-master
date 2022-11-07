package internal

import (
	"fmt"
	"github.com/agiledragon/gomonkey/v2"
	set "github.com/deckarep/golang-set"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
	"tinydfs-base/common"
	"tinydfs-base/util"
)

func TestMain(m *testing.M) {
	dataNodeMap = map[string]*DataNode{
		"dn1": {
			Id:            "dn1",
			status:        common.Alive,
			Address:       "dn1:6789",
			Chunks:        set.NewSetWith("chunk1", "chunk2", "chunk3"),
			Leases:        set.NewSetWith("chunk1"),
			HeartbeatTime: time.Time{},
		},
		"dn2": {
			Id:            "dn2",
			status:        common.Alive,
			Address:       "dn2:6789",
			Chunks:        set.NewSetWith("chunk1", "chunk2", "chunk3", "chunk4", "chunk5", "chunk6"),
			Leases:        set.NewSet(),
			HeartbeatTime: time.Time{},
		},
		"dn3": {
			Id:            "dn3",
			status:        common.Alive,
			Address:       "dn3:6789",
			Chunks:        set.NewSetWith("chunk1", "chunk2", "chunk4"),
			Leases:        set.NewSetWith("chunk4"),
			HeartbeatTime: time.Time{},
		},
		"dn4": {
			Id:            "dn4",
			status:        common.Alive,
			Address:       "dn4:6789",
			Chunks:        set.NewSetWith("chunk3", "chunk5", "chunk6"),
			Leases:        set.NewSet(),
			HeartbeatTime: time.Time{},
		},
		"dn5": {
			Id:            "dn5",
			status:        common.Alive,
			Address:       "dn5:6789",
			Chunks:        set.NewSetWith("chunk4", "chunk6"),
			Leases:        set.NewSet(),
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

func TestDegradeDataNode(t *testing.T) {
	type args struct {
		dataNodeId string
		stage      int
	}
	tests := []struct {
		name       string
		args       args
		Setup      func(t *testing.T)
		wantStatus int
		wantLen    int
	}{
		{
			name: "Degrade2Dead",
			args: args{
				dataNodeId: "dataNode1",
				stage:      common.Degrade2Dead,
			},
			Setup: func(t *testing.T) {
				dataNodeMap["dataNode1"] = &DataNode{
					Id:     "dataNode1",
					status: common.Waiting,
					Chunks: set.NewSet("chunk1", "chunk2", "chunk3"),
					FutureSendChunks: map[ChunkSendInfo]int{
						ChunkSendInfo{ChunkId: "chunk1", DataNodeId: "dataNode2"}: common.WaitToInform,
						ChunkSendInfo{ChunkId: "chunk1", DataNodeId: "dataNode3"}: common.WaitToInform,
						ChunkSendInfo{ChunkId: "chunk2", DataNodeId: "dataNode3"}: common.WaitToSend,
					},
				}
				batchClearDataNode := gomonkey.ApplyFunc(BatchClearDataNode, func(_ []interface{}, _ string) {})
				t.Cleanup(func() {
					batchClearDataNode.Reset()
					dataNodeMap = make(map[string]*DataNode)
					pendingChunkQueue = util.NewQueue[String]()
				})
			},
			wantStatus: common.Waiting,
			wantLen:    6,
		},
		{
			name: "Degrade2Waiting",
			args: args{
				dataNodeId: "dataNode1",
				stage:      common.Degrade2Waiting,
			},
			Setup: func(t *testing.T) {
				dataNodeMap["dataNode1"] = &DataNode{
					Id:     "dataNode1",
					status: common.Alive,
				}
				batchClearDataNode := gomonkey.ApplyFunc(BatchClearDataNode, func(_ []interface{}, _ string) {})
				t.Cleanup(func() {
					batchClearDataNode.Reset()
					dataNodeMap = make(map[string]*DataNode)
					pendingChunkQueue = util.NewQueue[String]()
				})
			},
			wantStatus: common.Waiting,
			wantLen:    0,
		},
	}

	// 执行测试用例
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.Setup != nil {
				tt.Setup(t)
			}
			DegradeDataNode(tt.args.dataNodeId, tt.args.stage)
			if tt.name == "Degrade2Waiting" {
				assert.Equal(t, tt.wantStatus, dataNodeMap[tt.args.dataNodeId].status, "Unexpected status.")
			}
			assert.Equal(t, tt.wantLen, pendingChunkQueue.Len(), "Unexpected len.")
		})
	}
}
