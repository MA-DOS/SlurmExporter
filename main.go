package main

import (
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"

	"github.com/MA-DOS/SlurmExporter/getData"
	"github.com/MA-DOS/SlurmExporter/log2prometheus"
)

func main() {
	fmt.Println("Hello, Slurm Exporter!")

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		logrus.Info("Starting server on port 9101")
		if err := http.ListenAndServe(":9101", nil); err != nil {
			logrus.Fatal(err)
		}
	}()

	for {
		output := getData.QueryJobs()
		jobs := getData.ParseJobOutput(output)
		if len(jobs) == 0 {
			fmt.Println("No jobs found")
		}
		getData.WriteToLog(jobs)
		log2prometheus.JobExporter(jobs)
		time.Sleep(2 * time.Second)
	}
}
