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

// CleanupRubbish checks dataNodeMap and chunkMap to remove the deleted chunks
// whose storing fileNode id is not found in directory tree.
func CleanupRubbish(ctx context.Context) {
	timer := time.NewTicker(time.Duration(viper.GetInt(common.CleanupTime)) * time.Second)
	for {
		select {
		case <-timer.C:
			data := getData4Apply(DataCheckOperation{Id: util.GenerateUUIDString()}, common.OperationDataCheck)
			GlobalMasterHandler.Raft.Apply(data, 5*time.Second)
		case <-ctx.Done():
			return
		}
	}
}

// DirectoryCheck checks directory tree to remove the deleted fileNode
// whose #{isDel} is true and #{delTime} has been pasted one day.
func DirectoryCheck(ctx context.Context) {
	timer := time.NewTicker(time.Duration(viper.GetInt(common.DirectoryCheckTime)) * time.Second)
	for {
		select {
		case <-timer.C:
			data := getData4Apply(TreeCheckOperation{Id: util.GenerateUUIDString()}, common.OperationTreeCheck)
			GlobalMasterHandler.Raft.Apply(data, 5*time.Second)
		case <-ctx.Done():
			return
		}
	}
}
