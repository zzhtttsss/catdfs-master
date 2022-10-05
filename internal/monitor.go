package internal

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/grpc"
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
