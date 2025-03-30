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
	parentJob := getData.ParseSlurmParentJob(getData.GetSlurmParentJob())
	return &SlurmJobCollector{
		JobID: prometheus.NewDesc("slurm_job_id",
			"Job ID of the slurm job",
			[]string{
				"partition", "job_name", "user", "node", "command", "priority", "cpus", "memory",
				"threads_per_core", "work_dir", "submit_time",
				"job_state", "run_time", "cpu_ids", "num_cpus", "num_tasks", "std_err", "std_in", "std_out",
				"max_vm_size", "ave_vm_size", "max_rss", "ave_rss", "ave_cpu", "ave_cpu_freq", "consumed_energy", "max_disk_read", "max_disk_write",
				"parent_job_pid",
			}, nil),
		CPUs:           prometheus.NewDesc("slurm_cpus", "Number of CPUs used by the job", []string{"job_id"}, nil),
		Memory:         prometheus.NewDesc("slurm_memory", "Memory used by the job", []string{"job_id"}, nil),
		SubmitTime:     prometheus.NewDesc("slurm_submit_time", "Time when the job was submitted", []string{"job_id"}, nil),
		ThreadsPerCore: prometheus.NewDesc("slurm_threads_per_core", "Number of threads per core", []string{"job_id"}, nil),
		RunTime:        prometheus.NewDesc("slurm_run_time", "Time the job has been running", []string{"job_id"}, nil),
		CPUIDs:         prometheus.NewDesc("slurm_cpu_ids", "IDs of the CPUs used by the job", []string{"job_id"}, nil),
		NumCPUs:        prometheus.NewDesc("slurm_num_cpus", "Number of CPUs used by the job", []string{"job_id"}, nil),
		NumTasks:       prometheus.NewDesc("slurm_num_tasks", "Number of tasks", []string{"job_id"}, nil),
		MaxVMSize:      prometheus.NewDesc("slurm_max_vm_size", "Maximum virtual memory size", []string{"job_id"}, nil),
		AveVMSize:      prometheus.NewDesc("slurm_ave_vm_size", "Average virtual memory size", []string{"job_id"}, nil),
		MaxRSS:         prometheus.NewDesc("slurm_max_rss", "Maximum resident set size", []string{"job_id"}, nil),
		AveRSS:         prometheus.NewDesc("slurm_ave_rss", "Average resident set size", []string{"job_id"}, nil),
		AveCPU:         prometheus.NewDesc("slurm_ave_cpu", "Average CPU usage", []string{"job_id"}, nil),
		AveCPUFreq:     prometheus.NewDesc("slurm_ave_cpu_freq", "Average CPU frequency", []string{"job_id"}, nil),
		ConsumedEnergy: prometheus.NewDesc("slurm_consumed_energy", "Energy consumed by the job", []string{"job_id"}, nil),
		MaxDiskRead:    prometheus.NewDesc("slurm_max_disk_read", "Maximum disk read", []string{"job_id"}, nil),
		MaxDiskWrite:   prometheus.NewDesc("slurm_max_disk_write", "Maximum disk write", []string{"job_id"}, nil),
		ParentJobPID:   prometheus.NewDesc("slurm_parent_job_pid", "Parent job PID", []string{"job_id"}, nil),
		ParentJobID:    parentJob,
	}
}

type SlurmJobCollector struct {
	JobID          *prometheus.Desc
	CPUs           *prometheus.Desc
	Memory         *prometheus.Desc
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
	AveCPU         *prometheus.Desc
	AveCPUFreq     *prometheus.Desc
	ConsumedEnergy *prometheus.Desc
	MaxDiskRead    *prometheus.Desc
	MaxDiskWrite   *prometheus.Desc
	ParentJobID    int
	ParentJobPID   *prometheus.Desc
}

func (cc *SlurmJobCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- cc.JobID
	ch <- cc.CPUs
	ch <- cc.Memory
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
	ch <- cc.AveCPU
	ch <- cc.AveCPUFreq
	ch <- cc.ConsumedEnergy
	ch <- cc.MaxDiskRead
	ch <- cc.MaxDiskWrite
	ch <- cc.ParentJobPID
}

func (cc *SlurmJobCollector) Collect(ch chan<- prometheus.Metric) {
	jobsMetrics := getData.SlurmJobsGetMetrics(cc.ParentJobID)
	for _, jm := range *jobsMetrics {
		labels := []string{
			jm.Partition, jm.JobName, jm.User, jm.Node,
			jm.Command, jm.Priority, strconv.Itoa(jm.CPUs), jm.Mem,
			jm.ThreadsPerCore, jm.WorkDir, jm.SubmitTime,
			jm.JobState, jm.RunTime, jm.CPU_IDs,
			strconv.Itoa(jm.NumCPUs), strconv.Itoa(jm.NumTasks),
			jm.StdErr, jm.StdIn, jm.Stdout, jm.MaxVMSize, jm.AveVMSize, jm.MaxRSS, jm.AveRSS,
			jm.AveCPU, jm.AveCPUFreq, jm.ConsumedEnergy, strconv.Itoa(jm.MaxDiskRead), strconv.Itoa(jm.MaxDiskWrite), strconv.Itoa(jm.ParentJobPID),
		}

		ch <- prometheus.MustNewConstMetric(cc.JobID, prometheus.GaugeValue, float64(jm.JobID), labels...)
		ch <- prometheus.MustNewConstMetric(cc.CPUs, prometheus.GaugeValue, float64(jm.CPUs), strconv.Itoa(jm.JobID))

		mem, err := strconv.ParseFloat(jm.Mem, 64)
		if err != nil {
			mem = 0
		}
		ch <- prometheus.MustNewConstMetric(cc.Memory, prometheus.GaugeValue, mem, strconv.Itoa(jm.JobID))

		submitTime, err := strconv.ParseFloat(jm.SubmitTime, 64)
		if err != nil {
			submitTime = 0
		}
		ch <- prometheus.MustNewConstMetric(cc.SubmitTime, prometheus.GaugeValue, submitTime, strconv.Itoa(jm.JobID))

		threadsPerCore, err := strconv.ParseFloat(jm.ThreadsPerCore, 64)
		if err != nil {
			threadsPerCore = 0
		}
		ch <- prometheus.MustNewConstMetric(cc.ThreadsPerCore, prometheus.GaugeValue, threadsPerCore, strconv.Itoa(jm.JobID))

		runTime, err := strconv.ParseFloat(jm.RunTime, 64)
		if err != nil {
			runTime = 0
		}
		ch <- prometheus.MustNewConstMetric(cc.RunTime, prometheus.GaugeValue, runTime, strconv.Itoa(jm.JobID))

		cpuIDs, err := strconv.ParseFloat(jm.CPU_IDs, 64)
		if err != nil {
			cpuIDs = 0
		}
		ch <- prometheus.MustNewConstMetric(cc.CPUIDs, prometheus.GaugeValue, cpuIDs, strconv.Itoa(jm.JobID))

		ch <- prometheus.MustNewConstMetric(cc.NumCPUs, prometheus.GaugeValue, float64(jm.NumCPUs), strconv.Itoa(jm.JobID))

		ch <- prometheus.MustNewConstMetric(cc.NumTasks, prometheus.GaugeValue, float64(jm.NumTasks), strconv.Itoa(jm.JobID))

		var maxVMSize float64
		if jm.MaxVMSize == "" {
			maxVMSize = 0
		} else {
			maxVMSize, err = strconv.ParseFloat(jm.MaxVMSize, 64)
			if err != nil {
				maxVMSize = 0
			}
		}
		ch <- prometheus.MustNewConstMetric(cc.MaxVMSize, prometheus.GaugeValue, maxVMSize, strconv.Itoa(jm.JobID))

		var ave_vm_size float64
		if jm.AveVMSize == "" {
			ave_vm_size = 0
		} else {
			ave_vm_size, err = strconv.ParseFloat(jm.AveVMSize, 64)
			if err != nil {
				ave_vm_size = 0
			}
		}
		ch <- prometheus.MustNewConstMetric(cc.AveVMSize, prometheus.GaugeValue, ave_vm_size, strconv.Itoa(jm.JobID))

		var maxRSS float64
		if jm.MaxRSS == "" {
			maxRSS = 0
		} else {
			maxRSS, err = strconv.ParseFloat(jm.MaxRSS, 64)
			if err != nil {
				maxRSS = 0
			}
		}
		ch <- prometheus.MustNewConstMetric(cc.MaxRSS, prometheus.GaugeValue, maxRSS, strconv.Itoa(jm.JobID))

		var aveRSS float64
		if jm.AveRSS == "" {
			aveRSS = 0
		} else {
			aveRSS, err = strconv.ParseFloat(jm.AveRSS, 64)
			if err != nil {
				aveRSS = 0
			}
		}
		ch <- prometheus.MustNewConstMetric(cc.AveRSS, prometheus.GaugeValue, aveRSS, strconv.Itoa(jm.JobID))

		var ave_cpu float64
		if jm.AveCPU == "" {
			ave_cpu = 0
		} else {
			ave_cpu, err = strconv.ParseFloat(jm.AveCPU, 64)
			if err != nil {
				ave_cpu = 0
			}
		}
		ch <- prometheus.MustNewConstMetric(cc.AveCPU, prometheus.GaugeValue, ave_cpu, strconv.Itoa(jm.JobID))

		var aveCPUFreq float64
		if jm.AveCPUFreq == "" {
			aveCPUFreq = 0
		} else {
			aveCPUFreq, err = strconv.ParseFloat(jm.AveCPUFreq, 64)
			if err != nil {
				aveCPUFreq = 0
			}
		}
		ch <- prometheus.MustNewConstMetric(cc.AveCPUFreq, prometheus.GaugeValue, aveCPUFreq, strconv.Itoa(jm.JobID))

		var consumedEnergy float64
		if jm.ConsumedEnergy == "" {
			consumedEnergy = 0
		} else {
			consumedEnergy, err = strconv.ParseFloat(jm.ConsumedEnergy, 64)
			if err != nil {
				consumedEnergy = 0
			}
		}
		ch <- prometheus.MustNewConstMetric(cc.ConsumedEnergy, prometheus.GaugeValue, consumedEnergy, strconv.Itoa(jm.JobID))

		ch <- prometheus.MustNewConstMetric(cc.MaxDiskRead, prometheus.GaugeValue, float64(jm.MaxDiskRead), strconv.Itoa(jm.JobID))
		ch <- prometheus.MustNewConstMetric(cc.MaxDiskWrite, prometheus.GaugeValue, float64(jm.MaxDiskWrite), strconv.Itoa(jm.JobID))
	}
}
