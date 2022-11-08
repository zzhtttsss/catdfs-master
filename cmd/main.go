package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
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
		logrus.Warnf("Http server error, Error detail %s", err)
	}
}
