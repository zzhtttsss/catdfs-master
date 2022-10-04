package main

import (
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"log"
	"net/http"
	"tinydfs-master/internal"
)

func init() {
	internal.CreateMasterHandler()
}

func main() {
	http.Handle("/metrics", promhttp.Handler())
	go internal.GlobalMasterHandler.Server()
	log.Println("Starting Listen 9101")
	err := http.ListenAndServe(":9101", nil)
	if err != nil {
		log.Println("port 9101", err)
	}
}
