package main

import (
	"fmt"
	"time"

	"github.com/MA-DOS/SlurmExporter/getData"
)

func main() {
	fmt.Println("Hello, Slurm Monitor!")
	slurmJobs := []getData.SlurmJob{}

	for {
		//output := getData.SlurmJobGetMetrics()
		//slurmJobs = append(slurmJobs, *output)
		slurmJobsPtr := getData.ParseSlurmControlMetrics(getData.SlurmControlData())
		slurmJobs = append(slurmJobs, *slurmJobsPtr)
		for _, job := range slurmJobs {
			fmt.Printf("Slurm Jobs: %+v\n", job)
		}
		time.Sleep(2 * time.Second)
	}
}

/*
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		logrus.Info("Starting server on port 9101")
		if err := http.ListenAndServe(":9101", nil); err != nil {
			logrus.Fatal(err)
		}
	}()
*/
/*
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
*/
