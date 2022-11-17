package internal

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/spf13/viper"
	"google.golang.org/grpc"
	"time"
	"tinydfs-base/common"
	"tinydfs-base/util"
)

const (
	Request = "request"
	Success = "success"
)

// MonitorHeartbeat runs in a goroutine. This function monitor heartbeat of
// all DataNode. It will check all DataNode in dataNodeMap every 1 minute,
// there are 3 situations:
// 1. We have received heartbeat of this DataNode in 30 seconds. if the Status
//    of it is waiting, we will set Status to alive, or we will do nothing.
// 2. The Status of DataNode is alive, and we have not received heartbeat of it
//    over 30 seconds, we will set Status to waiting.
// 3. The Status of DataNode is waiting, and we have not received heartbeat of it
//    over 10 minute, we will think this DataNode is dead and start a shrink.
func MonitorHeartbeat(ctx context.Context) {
	for {
		select {
		default:
			updateMapLock.Lock()
			for _, node := range dataNodeMap {
				Logger.WithContext(ctx).Debugf("Datanode id: %s, chunk set: %s", node.Id, node.Chunks.String())
				// Give died datanode a second chance to restart.
				if int(time.Now().Sub(node.HeartbeatTime).Seconds()) > viper.GetInt(common.ChunkWaitingTime)*
					viper.GetInt(common.ChunkHeartbeatTime) && node.Status == common.Alive {
					operation := &DegradeOperation{
						Id:         util.GenerateUUIDString(),
						DataNodeId: node.Id,
						Stage:      common.Degrade2Waiting,
					}
					data := getData4Apply(operation, common.OperationDegrade)
					_ = GlobalMasterHandler.Raft.Apply(data, 5*time.Second)
					continue
				}
				if int(time.Now().Sub(node.HeartbeatTime).Seconds()) > viper.GetInt(common.ChunkDieTime) &&
					node.Status == common.Waiting {
					csCountMonitor.Dec()
					operation := &DegradeOperation{
						Id:         util.GenerateUUIDString(),
						DataNodeId: node.Id,
						Stage:      common.Degrade2Dead,
					}
					data := getData4Apply(operation, common.OperationDegrade)
					_ = GlobalMasterHandler.Raft.Apply(data, 5*time.Second)
					continue
				}
			}
			updateMapLock.Unlock()
			Logger.WithContext(ctx).Infof("Complete a round of check, time: %s", time.Now().String())
			time.Sleep(time.Duration(viper.GetInt(common.MasterCheckTime)) * time.Second)
		case <-ctx.Done():
			return
		}
	}
}

// ConsumePendingChunk runs in a goroutine. This function will keep looping to
// check the pendingChunkQueue, there are 3 situations:
// 1. The timer is up, allocate all pending chunks in the pendingChunkQueue.
// 2. The lengths of the pendingChunkQueue is greater than or equal to the quantity
//    of a batch, allocate a batch of pending chunks in the pendingChunkQueue.
// 3. None of the above conditions are met，just do nothing.
func ConsumePendingChunk(ctx context.Context) {
	if pendingChunkQueue.Len() > 0 {
		BatchAllocateChunks()
	}
	timer := time.NewTicker(time.Duration(viper.GetInt(common.ChunkDeadChunkCheckTime)) * time.Second)
	for {
		select {
		case <-timer.C:
			BatchAllocateChunks()
		case <-ctx.Done():
			timer.Stop()
			return
		default:
			if pendingChunkQueue.Len() >= viper.GetInt(common.ChunkDeadChunkCopyThreshold) {
				BatchAllocateChunks()
			}
		}
	}
}

// CheckChunks checks dataNodeMap and chunkMap to remove the deleted chunks
// whose storing fileNode id is not found in directory tree.
func CheckChunks(ctx context.Context) {
	timer := time.NewTicker(time.Duration(viper.GetInt(common.CleanupTime)) * time.Second)
	for {
		select {
		case <-timer.C:
			data := getData4Apply(CheckChunksOperation{Id: util.GenerateUUIDString()}, common.OperationChunksCheck)
			GlobalMasterHandler.Raft.Apply(data, 5*time.Second)
		case <-ctx.Done():
			return
		}
	}
}

// CheckFileTree checks directory tree to remove the deleted fileNode whose
// #{isDel} is true and #{delTime} has been pasted one day.
func CheckFileTree(ctx context.Context) {
	timer := time.NewTicker(time.Duration(viper.GetInt(common.StorableCheckTime)) * time.Second)
	for {
		select {
		case <-timer.C:
			data := getData4Apply(CheckDataNodesOperation{Id: util.GenerateUUIDString()}, common.OperationFileTreeCheck)
			GlobalMasterHandler.Raft.Apply(data, 5*time.Second)
		case <-ctx.Done():
			return
		}
	}
}

// CheckStorableDataNode checks how many DataNode is storable which means its
// disk usage is below the threshold.
func CheckStorableDataNode(ctx context.Context) {
	timer := time.NewTicker(time.Duration(viper.GetInt(common.DirectoryCheckTime)) * time.Second)
	for {
		select {
		case <-timer.C:
			data := getData4Apply(CheckDataNodesOperation{Id: util.GenerateUUIDString()}, common.OperationDataNodesCheck)
			GlobalMasterHandler.Raft.Apply(data, 5*time.Second)
		case <-ctx.Done():
			return
		}
	}
}

var (
	csCountMonitor = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "chunkserver_count",
		Help: "the number of chunkserver",
	})
	rpcCountMonitor = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "rpc_count",
		Help: "the number of rpc call",
	})
	rpcFromClientCountMonitor = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "rpc_from_client_count",
		Help: "the number of rpc call from client",
	}, []string{"addr", "op", "type"})
	rpcSuccessCountMonitor = promauto.NewCounter(prometheus.CounterOpts{
		Name: "rpc_success_count",
		Help: "the number of successful rpc call",
	})

	interceptor grpc.UnaryServerInterceptor = func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler) (resp interface{}, err error) {
		rpcCountMonitor.Inc()
		rep, err := handler(ctx, req)
		if err == nil {
			rpcSuccessCountMonitor.Inc()
		}
		return rep, err
	}
)

func RequestCountInc(addr, op string) {
	rpcFromClientCountMonitor.WithLabelValues(addr, op, Request).Inc()
}

func SuccessCountInc(addr, op string) {
	rpcFromClientCountMonitor.WithLabelValues(addr, op, Success).Inc()
}
