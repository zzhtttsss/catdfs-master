package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
	"tinydfs-base/common"
	"tinydfs-master/internal"
)

const (
	MetricsServerPort = "9101"
)

func init() {
	internal.CreateMasterHandler()
}

func main() {
	go internal.GlobalMasterHandler.Server()
	http.Handle("/metrics", promhttp.HandlerFor(
		prometheus.DefaultGatherer,
		promhttp.HandlerOpts{
			EnableOpenMetrics: true,
		},
	))
	err := http.ListenAndServe(common.AddressDelimiter+MetricsServerPort, nil)
	if err != nil {
		log.Println("Http server error, port 9101", err)
	}
}
