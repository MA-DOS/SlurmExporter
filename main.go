package main

import (
	"flag"
	"fmt"
	"net/http"

	"github.com/MA-DOS/SlurmExporter/getData"
	"github.com/MA-DOS/SlurmExporter/log2prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

func init() {
	// Initialize Prometheus Exporter.
	prometheus.MustRegister(log2prometheus.NewSlurmJobCollector())
}

var listenAddress = flag.String(
	"listen-address",
	":8082",
	"The address to listen on for HTTP requests.",
)

func main() {
	flag.Parse()
	fmt.Println("Hello, Slurm Exporter!")

	// Start the parent job watcher in the background
	getData.StartParentJobWatcher()

	// Start the HTTP server
	logrus.Infof("Starting Server on port %s", *listenAddress)
	http.Handle("/metrics", promhttp.Handler())
	logrus.Fatal(http.ListenAndServe(*listenAddress, nil))
}
