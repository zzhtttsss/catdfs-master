package internal

import (
	"context"
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
