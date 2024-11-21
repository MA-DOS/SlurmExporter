package log2prometheus

import (
	"strconv"

	"github.com/MA-DOS/SlurmExporter/getData"
	"github.com/prometheus/client_golang/prometheus"
)

// Defining metrics for prometheus
var (
	jobID = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "slurm_job_id",
			Help: "Job ID of the slurm job",
		},
		[]string{"partition", "name", "user"},
	)
	energyConsumption = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "slurm_job_energy_consumption",
			Help: "Energy consumption of the slurm job",
		},
		[]string{"job_id"},
	)
)

// Init func is called automatically at package load time to register the metrics
func init() {
	prometheus.MustRegister(jobID)
	prometheus.MustRegister(energyConsumption)
}

func JobExporter(jobs []getData.SlurmJob) {
	for _, job := range jobs {
		jobID.WithLabelValues(job.Partition, job.Name, job.User).Set(float64(job.JobID))
		energyConsumption.WithLabelValues(strconv.Itoa(job.JobID)).Set(float64(job.EnergyConsumption))
	}
}
