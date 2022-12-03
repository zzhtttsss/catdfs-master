package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	"tinydfs-base/common"
	"tinydfs-master/internal"
)

const (
	MetricsServerPort = "9101"
)

func main() {
	internal.CreateMasterHandler()
	go internal.GlobalMasterHandler.Server()
	http.Handle("/metrics", promhttp.HandlerFor(
		prometheus.DefaultGatherer,
		promhttp.HandlerOpts{
			EnableOpenMetrics: true,
		},
	))
	err := http.ListenAndServe(common.AddressDelimiter+MetricsServerPort, nil)
	if err != nil {
		internal.Logger.Warnf("Http server error, Error detail %s", err)
	}
}
