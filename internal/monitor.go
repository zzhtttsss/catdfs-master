package internal

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
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
	csChunkNumberMonitor = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "chunkserver_chunk_number",
		Help: "the number of chunks in chunkserver",
	})
	rpcCountMonitor = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "rpc_count",
		Help: "the number of rpc call",
	})
	rpcFromClientCountMonitor = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "rpc_from_client_count",
		Help: "the number of rpc call from client",
	}, []string{"op", "type"})
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

func RequestCountInc(op string) {
	rpcFromClientCountMonitor.WithLabelValues(op, Request).Inc()
}

func SuccessCountInc(op string) {
	rpcFromClientCountMonitor.WithLabelValues(op, Success).Inc()
}