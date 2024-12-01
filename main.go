package main

import (
	"fmt"
	"time"

	"github.com/MA-DOS/SlurmExporter/getData"
	"github.com/sirupsen/logrus"
)

func main() {
	fmt.Println("Hello, Slurm Monitor!")

	for {
		//getData.SlurmJobGetMetrics()
		qm := getData.ParseSlurmQueueMetrics(getData.SlurmQueueData())
		cm := getData.ParseSlurmControlMetrics(getData.SlurmControlData())
		metricMap := getData.AggregateSlurmMetrics(qm, cm)

		/* 				for jobID, metrics := range metricMap {
			fmt.Printf("JobID: %s\n, Metrics: %+v\n", jobID, metrics)

		} */

		var slurmJob getData.SlurmJob

		result := slurmJob.NewSlurmJobStruct(metricMap)

		for _, job := range *result {
			logrus.Infof("Slurm Jobs: %+v\n", job)
			//printStructFields(job)
		}

		/* 	queueOutput := getData.ParseSlurmQueueMetrics(getData.SlurmQueueData())
		fmt.Printf("Queue: %+v\n", queueOutput)
		controlOutput := getData.ParseSlurmControlMetrics(getData.SlurmControlData())

		for jobID, metrics := range controlOutput {
		fmt.Printf("JobID: %s, Metrics: %+v\n", jobID, metrics)
		}  */
		time.Sleep(10 * time.Second)
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
