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
	// Define a consistent set of labels for all metrics
	commonLabels := []string{
		"partition", "job_name", "user", "node",
		"command", "priority", "cpus", "memory",
		"threads_per_core", "work_dir", "submit_time",
		"job_state", "run_time", "cpu_ids",
		"num_cpus", "num_tasks",
		"std_err", "std_in", "std_out", "max_vm_size", "ave_vm_size", "max_rss", "ave_rss",
		"consumed_energy", "max_disk_read", "max_disk_write", "slurm_job_pid",
	}

	return &SlurmJobCollector{
		JobID: prometheus.NewDesc("slurm_job_id",
			"Job ID of the slurm job",
			commonLabels, nil),
		CPUs: prometheus.NewDesc("slurm_cpus",
			"Number of CPUs used by the job",
			commonLabels, nil),
		// Memory: prometheus.NewDesc("slurm_memory",
		// 	"Memory used by the job",
		// 	commonLabels, nil),
		SubmitTime: prometheus.NewDesc("slurm_submit_time",
			"Time when the job was submitted",
			commonLabels, nil),
		ThreadsPerCore: prometheus.NewDesc("slurm_threads_per_core",
			"Number of threads per core",
			commonLabels, nil),
		RunTime: prometheus.NewDesc("slurm_run_time",
			"Time the job has been running",
			commonLabels, nil),
		CPUIDs: prometheus.NewDesc("slurm_cpu_ids",
			"IDs of the CPUs used by the job",
			commonLabels, nil),
		NumCPUs: prometheus.NewDesc("slurm_num_cpus",
			"Number of CPUs used by the job",
			commonLabels, nil),
		NumTasks: prometheus.NewDesc("slurm_num_tasks",
			"Number of tasks",
			commonLabels, nil),
		MaxVMSize: prometheus.NewDesc("slurm_max_vm_size",
			"Maximum virtual memory size",
			commonLabels, nil),
		AveVMSize: prometheus.NewDesc("slurm_ave_vm_size",
			"Average virtual memory size",
			commonLabels, nil),
		MaxRSS: prometheus.NewDesc("slurm_max_rss",
			"Maximum resident set size",
			commonLabels, nil),
		AveRSS: prometheus.NewDesc("slurm_ave_rss",
			"Average resident set size",
			commonLabels, nil),
		// AveCPU: prometheus.NewDesc("slurm_ave_cpu",
		// 	"Average CPU usage",
		// 	commonLabels, nil),
		// AveCPUFreq: prometheus.NewDesc("slurm_ave_cpu_freq",
		// 	"Average CPU frequency",
		// 	commonLabels, nil),
		ConsumedEnergy: prometheus.NewDesc("slurm_consumed_energy",
			"Energy consumed by the job",
			commonLabels, nil),
		MaxDiskRead: prometheus.NewDesc("slurm_max_disk_read",
			"Maximum disk read",
			commonLabels, nil),
		MaxDiskWrite: prometheus.NewDesc("slurm_max_disk_write",
			"Maximum disk write",
			commonLabels, nil),
		JobPID: prometheus.NewDesc("slurm_job_pid",
			"Job PID",
			commonLabels, nil),
		// ParentJobID: parentJob,
	}
}

type SlurmJobCollector struct {
	JobID *prometheus.Desc
	CPUs  *prometheus.Desc
	// Memory         *prometheus.Desc
	SubmitTime     *prometheus.Desc
	ThreadsPerCore *prometheus.Desc
	RunTime        *prometheus.Desc
	CPUIDs         *prometheus.Desc
	NumCPUs        *prometheus.Desc
	NumTasks       *prometheus.Desc
	MaxVMSize      *prometheus.Desc
	AveVMSize      *prometheus.Desc
	MaxRSS         *prometheus.Desc
	AveRSS         *prometheus.Desc
	// AveCPU         *prometheus.Desc
	// AveCPUFreq     *prometheus.Desc
	ConsumedEnergy *prometheus.Desc
	MaxDiskRead    *prometheus.Desc
	MaxDiskWrite   *prometheus.Desc
	// ParentJobID    int
	JobPID *prometheus.Desc
}

func (cc *SlurmJobCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- cc.JobID
	ch <- cc.CPUs
	// ch <- cc.Memory
	ch <- cc.SubmitTime
	ch <- cc.ThreadsPerCore
	ch <- cc.RunTime
	ch <- cc.CPUIDs
	ch <- cc.NumCPUs
	ch <- cc.NumTasks
	ch <- cc.MaxVMSize
	ch <- cc.AveVMSize
	ch <- cc.MaxRSS
	ch <- cc.AveRSS
	// ch <- cc.AveCPU
	// ch <- cc.AveCPUFreq
	ch <- cc.ConsumedEnergy
	ch <- cc.MaxDiskRead
	ch <- cc.MaxDiskWrite
	ch <- cc.JobPID
}

func (cc *SlurmJobCollector) Collect(ch chan<- prometheus.Metric) {
	parentJob := getData.GetParentJob()
	jobsMetrics := getData.SlurmJobsGetMetrics(parentJob)

	// Collect the available labels.
	for _, jm := range *jobsMetrics {
		// Includes all available labels.
		allLabels := []string{
			jm.Partition, jm.JobName, jm.User, jm.Node,
			jm.Command, jm.Priority, strconv.Itoa(jm.CPUs), jm.Mem,
			jm.ThreadsPerCore, jm.WorkDir, jm.SubmitTime,
			jm.JobState, jm.RunTime, jm.CPU_IDs,
			strconv.Itoa(jm.NumCPUs), strconv.Itoa(jm.NumTasks),
			jm.StdErr, jm.StdIn, jm.Stdout, jm.MaxVMSize, jm.AveVMSize, jm.MaxRSS, jm.AveRSS,
			jm.ConsumedEnergy, strconv.Itoa(jm.MaxDiskRead), strconv.Itoa(jm.MaxDiskWrite), strconv.Itoa(jm.JobPID),
		}

		// CPU Metric Labels
		// cpuLables := []string{
		// 	strconv.Itoa(jm.JobID), jm.CPU_IDs, strconv.Itoa(jm.CPUs), jm.ConsumedEnergy, jm.ThreadsPerCore,
		// }

		// // Memory Metric Labels
		// memLabels := []string{
		// 	strconv.Itoa(jm.JobID), jm.MaxVMSize, jm.AveVMSize, jm.MaxRSS, jm.AveRSS,
		// }

		// // Number of Task Labels
		// numTaskLabels := []string{
		// 	strconv.Itoa(jm.JobID), jm.CPU_IDs, strconv.Itoa(jm.NumCPUs), jm.ConsumedEnergy, strconv.Itoa(jm.JobID), jm.RunTime, jm.SubmitTime}

		// // MaxVMSize Labels
		// maxVMSizeLabels := []string{
		// 	strconv.Itoa(jm.JobID), jm.MaxVMSize, jm.AveVMSize, jm.MaxRSS, jm.AveRSS, jm.Mem}

		// // AvgCPUFreq Labels
		// avgCPUFreqLabels := []string{
		// 	strconv.Itoa(jm.JobID), strconv.Itoa(jm.NumCPUs), jm.ConsumedEnergy, jm.ThreadsPerCore, strconv.Itoa(jm.NumTasks), jm.CPU_IDs}

		// // Consumed Energy Labels
		// consumedEnergyLabels := []string{
		// 	strconv.Itoa(jm.JobID), strconv.Itoa(jm.JobPID)}

		// Assign the metrics including their labels to the data channel.
		// Metric for job ID including all available labels.
		ch <- prometheus.MustNewConstMetric(cc.JobID, prometheus.GaugeValue, float64(jm.JobID), allLabels...)

		// Metric for amount of CPUs per job including jobID, CPU IDs, consumed energy, TPC.
		ch <- prometheus.MustNewConstMetric(cc.NumCPUs, prometheus.GaugeValue, float64(jm.NumCPUs), allLabels...)

		// Metric for Number of Tasks inlcuding jobID, CPU IDs, consumed energy, run time, submit time.
		ch <- prometheus.MustNewConstMetric(cc.NumTasks, prometheus.GaugeValue, float64(jm.NumTasks), allLabels...)

		// Metric for the MaxVMSize per job including jobID, MaxVMSize, AveVMSize, MaxRSS, AveRSS.
		var maxVMSize float64
		maxVMSize, _ = strconv.ParseFloat(jm.MaxVMSize, 64)
		ch <- prometheus.MustNewConstMetric(cc.MaxVMSize, prometheus.GaugeValue, maxVMSize, allLabels...)

		var ave_vm_size float64

		// Metric for the AveVMSize per job including jobID, MaxVMSize, AveVMSize, MaxRSS, AveRSS (use the same labels as MaxVMSize).
		ave_vm_size, _ = strconv.ParseFloat(jm.AveVMSize, 64)
		ch <- prometheus.MustNewConstMetric(cc.AveVMSize, prometheus.GaugeValue, ave_vm_size, allLabels...)

		// Metric for the MaxRSS per job including jobID, MaxVMSize, AveVMSize, MaxRSS, AveRSS (use the same labels as MaxVMSize).
		var maxRSS float64
		maxRSS, _ = strconv.ParseFloat(jm.MaxRSS, 64)
		ch <- prometheus.MustNewConstMetric(cc.MaxRSS, prometheus.GaugeValue, maxRSS, allLabels...)

		// Metric for the AveRSS per job including jobID, MaxVMSize, AveVMSize, MaxRSS, AveRSS (use the same labels as MaxVMSize).
		var aveRSS float64
		aveRSS, _ = strconv.ParseFloat(jm.AveRSS, 64)
		ch <- prometheus.MustNewConstMetric(cc.AveRSS, prometheus.GaugeValue, aveRSS, allLabels...)

		// Metric for the consumed energy per job inlcuding the jobID, the job PID.
		var consumedEnergy float64
		consumedEnergy, _ = strconv.ParseFloat(jm.ConsumedEnergy, 64)
		ch <- prometheus.MustNewConstMetric(cc.ConsumedEnergy, prometheus.GaugeValue, consumedEnergy, allLabels...)

		// Metric for the MaxDiskRead per job including the jobID, the job PID (from the consumed energy labels).
		ch <- prometheus.MustNewConstMetric(cc.MaxDiskRead, prometheus.GaugeValue, float64(jm.MaxDiskRead), allLabels...)

		// Metric for the MaxDiskWrite per job including the jobID, the job PID (from the consumed energy labels).i
		ch <- prometheus.MustNewConstMetric(cc.MaxDiskWrite, prometheus.GaugeValue, float64(jm.MaxDiskWrite), allLabels...)
	}
}
