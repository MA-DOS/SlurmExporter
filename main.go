package main

import (
	"fmt"
	"time"

	"github.com/MA-DOS/SlurmExporter/getData"
)

func main() {
	fmt.Println("Hello, Slurm Exporter!")

	for {
		output := getData.QueryJobs()
		jobs := getData.ParseJobOutput(output)
		if len(jobs) == 0 {
			fmt.Println("No jobs found")
		}
		getData.WriteToLog(jobs)
		time.Sleep(2 * time.Second)
	}
}
