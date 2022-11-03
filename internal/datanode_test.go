package internal

import (
	"context"
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
	m.Run()
}

func TestGetDataNode2MoveChunk(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func(ctx context.Context) {
		defer fmt.Println("子routine退出")
		for {
			select {
			case <-ctx.Done():
				fmt.Println("收到退出信号")
				return
			default:
				fmt.Println("执行一项耗时任务")
				time.Sleep(5 * time.Second)
				fmt.Println("执行完毕")
				break
			}
		}
	}(ctx)
	time.Sleep(10 * time.Second)
	cancel()
	time.Sleep(10 * time.Second)
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
