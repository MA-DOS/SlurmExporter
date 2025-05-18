package main

import (
	"flag"
	"net/http"

	"github.com/MA-DOS/SlurmExporter/getData"
	"github.com/MA-DOS/SlurmExporter/log2prometheus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
)

var listenAddress = flag.String(
	"listen-address",
	":8082",
	"The address to listen on for HTTP requests.",
)

func main() {
	flag.Parse()

	// Create a channel to signal when the parent job is found
	done := make(chan bool)

	// Start the parent job watcher in the background
	getData.ObserveSlurmJobs(done)

	// Wait for the parent job to be found
	collector := log2prometheus.NewSlurmJobCollector()

	<-done
	logrus.Info("Initialized sbatch step. Starting HTTP server.")

	prometheus.MustRegister(collector)

	// Start the HTTP server
	logrus.Infof("Starting Slurm Exporter on port %s", *listenAddress)
	http.Handle("/metrics", promhttp.Handler())
	logrus.Fatal(http.ListenAndServe(*listenAddress, nil))
}
