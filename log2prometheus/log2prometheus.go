package log2prometheus

import (
	"strconv"

	"github.com/MA-DOS/SlurmExporter/getData"
	"github.com/prometheus/client_golang/prometheus"
)

/*
 * Implement the Prometheus Collector interface and feed the
 * Slurm scheduler metrics into it.
 * https://godoc.org/github.com/prometheus/client_golang/prometheus#Collector
 */

func NewSlurmJobCollector() *SlurmJobCollector {
	return &SlurmJobCollector{
		JobID: prometheus.NewDesc("slurm_job_id",
			"Job ID of the slurm job",
			[]string{
				"partition", "job_name", "user", "node", "command", "priority", "cpus", "memory",
				"threads_per_core", "work_dir", "submit_time",
				"job_state", "run_time", "cpu_ids", "num_cpus", "num_tasks", "std_err", "std_in", "std_out",
				"max_vm_size", "ave_vm_size", "max_rss", "ave_rss", "ave_cpu", "ave_cpu_freq", "consumed_energy", "max_disk_read", "max_disk_write",
			}, nil),
	}
}

type SlurmJobCollector struct {
	JobID *prometheus.Desc
}

func (cc *SlurmJobCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- cc.JobID
}

func (cc *SlurmJobCollector) Collect(ch chan<- prometheus.Metric) {
	jobsMetrics := getData.SlurmJobsGetMetrics()
	for _, jm := range *jobsMetrics {
		labels := []string{
			jm.Partition, jm.JobName, jm.User, jm.Node,
			jm.Command, jm.Priority, strconv.Itoa(jm.CPUs), (jm.Mem),
			jm.ThreadsPerCore, jm.WorkDir, jm.SubmitTime,
			jm.JobState, jm.RunTime, jm.CPU_IDs,
			strconv.Itoa(jm.NumCPUs), strconv.Itoa(jm.NumTasks),
			jm.StdErr, jm.StdIn, jm.Stdout, jm.MaxVMSize, jm.AveVMSize, jm.MaxRSS, jm.AveRSS,
			jm.AveCPU, jm.AveCPUFreq, jm.ConsumedEnergy, strconv.Itoa(jm.MaxDiskRead), strconv.Itoa(jm.MaxDiskWrite),
		}

		ch <- prometheus.MustNewConstMetric(cc.JobID, prometheus.GaugeValue, float64(jm.JobID), labels...)
	}
}
