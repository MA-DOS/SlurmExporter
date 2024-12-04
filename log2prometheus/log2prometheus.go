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
			[]string{"job_id", "partition", "job_name", "user", "node"}, nil),
		JobName: prometheus.NewDesc("slurm_job_name",
			"Name of the slurm job",
			[]string{"job_id", "partition", "job_name", "user", "node"}, nil),
		User: prometheus.NewDesc("slurm_job_user",
			"User of the slurm job",
			[]string{"job_id", "partition", "job_name", "user", "node"}, nil),
		Node: prometheus.NewDesc("slurm_job_node",
			"Node of the slurm job",
			[]string{"job_id", "partition", "job_name", "user", "node"}, nil),
		MinCPUs: prometheus.NewDesc("slurm_job_min_cpu",
			"Minimum CPU of the slurm job",
			[]string{"min_cpus", "partition", "job_name", "user", "node"}, nil),
		MinTmpDisk: prometheus.NewDesc("slurm_job_min_tmp_disk",
			"Minimum temporary disk of the slurm job",
			[]string{"min_tmp_disk", "partition", "job_name", "user", "node"}, nil),
		EndTime: prometheus.NewDesc("slurm_job_end_time",
			"End time of the slurm job",
			[]string{"end_time", "partition", "job_name", "user", "node"}, nil),
		TimeLimit: prometheus.NewDesc("slurm_job_time_limit",
			"Time limit of the slurm job",
			[]string{"time_limit", "partition", "job_name", "user", "node"}, nil),
		CoresPerSocket: prometheus.NewDesc("slurm_job_cores_per_socket",
			"Cores per socket of the slurm job",
			[]string{"cores_per_socket", "partition", "job_name", "user", "node"}, nil),
		MinMemory: prometheus.NewDesc("slurm_job_min_memory",
			"Minimum memory of the slurm job",
			[]string{"min_memory", "partition", "job_name", "user", "node"}, nil),
		Command: prometheus.NewDesc("slurm_job_command",
			"Command of the slurm job",
			[]string{"command", "partition", "job_name", "user", "node"}, nil),
		Priority: prometheus.NewDesc("slurm_job_priority",
			"Priority of the slurm job",
			[]string{"priority", "partition", "job_name", "user", "node"}, nil),
		Reason: prometheus.NewDesc("slurm_job_reason",
			"Reason of the slurm job",
			[]string{"reason", "partition", "job_name", "user", "node"}, nil),
		State: prometheus.NewDesc("slurm_job_state",
			"State of the slurm job",
			[]string{"state", "partition", "job_name", "user", "node"}, nil),
		SCT: prometheus.NewDesc("slurm_job_sct",
			"SCT of the slurm job",
			[]string{"sockets_cores_threads", "partition", "job_name", "user", "node"}, nil),
		CPUs: prometheus.NewDesc("slurm_job_cpus",
			"CPUs of the slurm job",
			[]string{"cpus", "partition", "job_name", "user", "node"}, nil),
		Dependency: prometheus.NewDesc("slurm_job_dependency",
			"Dependency of the slurm job",
			[]string{"dependency", "partition", "job_name", "user", "node"}, nil),
		SocketsPerNode: prometheus.NewDesc("slurm_job_sockets_per_node",
			"Sockets per node of the slurm job",
			[]string{"sockets_per_node", "partition", "job_name", "user", "node"}, nil),
		ThreadsPerCore: prometheus.NewDesc("slurm_job_threads_per_core",
			"Threads per core of the slurm job",
			[]string{"threads_per_core", "partition", "job_name", "user", "node"}, nil),
		TimeLeft: prometheus.NewDesc("slurm_job_time_left",
			"Time left of the slurm job",
			[]string{"time_left", "partition", "job_name", "user", "node"}, nil),
		WorkDir: prometheus.NewDesc("slurm_job_work_dir",
			"Work directory of the slurm job",
			[]string{"work_dir", "partition", "job_name", "user", "node"}, nil),
		SubmitTime: prometheus.NewDesc("slurm_job_submit_time",
			"Submit time of the slurm job",
			[]string{"submit_time", "partition", "job_name", "user", "node"}, nil),
		JobState: prometheus.NewDesc("slurm_job_job_state",
			"Job state of the slurm job",
			[]string{"job_state", "partition", "job_name", "user", "node"}, nil),
		RunTime: prometheus.NewDesc("slurm_job_run_time",
			"Run time of the slurm job",
			[]string{"run_time", "partition", "job_name", "user", "node"}, nil),
		EligibleTime: prometheus.NewDesc("slurm_job_eligible_time",
			"Eligible time of the slurm job",
			[]string{"eligible_time", "partition", "job_name", "user", "node"}, nil),
		AccrueTime: prometheus.NewDesc("slurm_job_accrue_time",
			"Accrue time of the slurm job",
			[]string{"accrue_time", "partition", "job_name", "user", "node"}, nil),
		SuspendTime: prometheus.NewDesc("slurm_job_suspend_time",
			"Suspend time of the slurm job",
			[]string{"suspend_time", "partition", "job_name", "user", "node"}, nil),
		EndTime2: prometheus.NewDesc("slurm_job_end_time2",
			"End time of the slurm job",
			[]string{"end_time_2", "partition", "job_name", "user", "node"}, nil),
		CPU_IDs: prometheus.NewDesc("slurm_job_cpu_ids",
			"CPU IDs of the slurm job",
			[]string{"cpu_ids", "partition", "job_name", "user", "node"}, nil),
		NumCPUs: prometheus.NewDesc("slurm_job_num_cpus",
			"Number of CPUs of the slurm job",
			[]string{"num_cpus", "partition", "job_name", "user", "node"}, nil),
		NumTasks: prometheus.NewDesc("slurm_job_num_tasks",
			"Number of tasks of the slurm job",
			[]string{"num_tasks", "partition", "job_name", "user", "node"}, nil),
		CPUSperTask: prometheus.NewDesc("slurm_job_cpus_per_task",
			"CPUs per task of the slurm job",
			[]string{"cpus_per_task", "partition", "job_name", "user", "node"}, nil),
		MinMemoryCPU: prometheus.NewDesc("slurm_job_min_memory_cpu",
			"Minimum memory of the slurm job",
			[]string{"min_memory_cpu", "partition", "job_name", "user", "node"}, nil),
		MemoryAssigned: prometheus.NewDesc("slurm_job_memory_assigned",
			"Memory assigned of the slurm job",
			[]string{"memory_assigned", "partition", "job_name", "user", "node"}, nil),
		StdErr: prometheus.NewDesc("slurm_job_std_err",
			"Standard error of the slurm job",
			[]string{"std_err", "partition", "job_name", "user", "node"}, nil),
		StdIn: prometheus.NewDesc("slurm_job_std_in",
			"Standard input of the slurm job",
			[]string{"std_in", "partition", "job_name", "user", "node"}, nil),
		StdOut: prometheus.NewDesc("slurm_job_std_out",
			"Standard output of the slurm job",
			[]string{"std_out", "partition", "job_name", "user", "node"}, nil),
	}
}

type SlurmJobCollector struct {
	JobID          *prometheus.Desc
	JobName        *prometheus.Desc
	User           *prometheus.Desc
	Node           *prometheus.Desc
	MinCPUs        *prometheus.Desc
	MinTmpDisk     *prometheus.Desc
	EndTime        *prometheus.Desc
	TimeLimit      *prometheus.Desc
	CoresPerSocket *prometheus.Desc
	MinMemory      *prometheus.Desc
	Command        *prometheus.Desc
	Priority       *prometheus.Desc
	Reason         *prometheus.Desc
	State          *prometheus.Desc
	SCT            *prometheus.Desc
	CPUs           *prometheus.Desc
	Dependency     *prometheus.Desc
	SocketsPerNode *prometheus.Desc
	ThreadsPerCore *prometheus.Desc
	TimeLeft       *prometheus.Desc
	WorkDir        *prometheus.Desc
	SubmitTime     *prometheus.Desc
	JobState       *prometheus.Desc
	RunTime        *prometheus.Desc
	EligibleTime   *prometheus.Desc
	AccrueTime     *prometheus.Desc
	SuspendTime    *prometheus.Desc
	EndTime2       *prometheus.Desc
	CPU_IDs        *prometheus.Desc
	NumCPUs        *prometheus.Desc
	NumTasks       *prometheus.Desc
	CPUSperTask    *prometheus.Desc
	MinMemoryCPU   *prometheus.Desc
	MemoryAssigned *prometheus.Desc
	StdErr         *prometheus.Desc
	StdIn          *prometheus.Desc
	StdOut         *prometheus.Desc
}

// Send all metric descriptions
func (c *SlurmJobCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.JobID
	ch <- c.JobName
	ch <- c.User
	ch <- c.Node
	ch <- c.MinCPUs
	ch <- c.MinTmpDisk
	ch <- c.EndTime
	ch <- c.TimeLimit
	ch <- c.CoresPerSocket
	ch <- c.MinMemory
	ch <- c.Command
	ch <- c.Priority
	ch <- c.Reason
	ch <- c.State
	ch <- c.SCT
	ch <- c.CPUs
	ch <- c.Dependency
	ch <- c.SocketsPerNode
	ch <- c.ThreadsPerCore
	ch <- c.TimeLeft
	ch <- c.WorkDir
	ch <- c.SubmitTime
	ch <- c.JobState
	ch <- c.RunTime
	ch <- c.EligibleTime
	ch <- c.AccrueTime
	ch <- c.SuspendTime
	ch <- c.EndTime2
	ch <- c.CPU_IDs
	ch <- c.NumCPUs
	ch <- c.NumTasks
	ch <- c.CPUSperTask
	ch <- c.MinMemoryCPU
	ch <- c.MemoryAssigned
	ch <- c.StdErr
	ch <- c.StdIn
	ch <- c.StdOut
}

func (cc *SlurmJobCollector) Collect(ch chan<- prometheus.Metric) {
	jobsMetrics := getData.SlurmJobsGetMetrics()
	for _, jm := range *jobsMetrics {
		jobIDstr := strconv.Itoa(jm.JobID)
		ch <- prometheus.MustNewConstMetric(cc.JobID, prometheus.GaugeValue, float64(jm.JobID), jobIDstr, jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.JobName, prometheus.GaugeValue, float64(jm.JobID), jobIDstr, jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.User, prometheus.GaugeValue, float64(jm.JobID), jobIDstr, jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.Node, prometheus.GaugeValue, float64(jm.JobID), jobIDstr, jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.MinCPUs, prometheus.GaugeValue, float64(jm.JobID), strconv.Itoa(jm.MinCPUs), jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.MinTmpDisk, prometheus.GaugeValue, float64(jm.JobID), strconv.Itoa(jm.MinTmpDisk), jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.EndTime, prometheus.GaugeValue, float64(jm.JobID), jm.EndTime, jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.TimeLimit, prometheus.GaugeValue, float64(jm.JobID), jm.TimeLimit, jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.CoresPerSocket, prometheus.GaugeValue, float64(jm.JobID), jm.CoresPerSocket, jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.MinMemory, prometheus.GaugeValue, float64(jm.JobID), jm.MinMemory, jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.Command, prometheus.GaugeValue, float64(jm.JobID), jm.Command, jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.Priority, prometheus.GaugeValue, float64(jm.JobID), jm.Priority, jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.Reason, prometheus.GaugeValue, float64(jm.JobID), jm.Reason, jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.State, prometheus.GaugeValue, float64(jm.JobID), jm.State, jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.SCT, prometheus.GaugeValue, float64(jm.JobID), jm.SCT, jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.CPUs, prometheus.GaugeValue, float64(jm.JobID), strconv.Itoa(jm.CPUs), jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.Dependency, prometheus.GaugeValue, float64(jm.JobID), jm.Dependency, jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.SocketsPerNode, prometheus.GaugeValue, float64(jm.JobID), jm.SocketsPerNode, jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.ThreadsPerCore, prometheus.GaugeValue, float64(jm.JobID), jm.ThreadsPerCore, jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.TimeLeft, prometheus.GaugeValue, float64(jm.JobID), jm.TimeLimit, jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.WorkDir, prometheus.GaugeValue, float64(jm.JobID), jm.WorkDir, jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.SubmitTime, prometheus.GaugeValue, float64(jm.JobID), jm.SubmitTime, jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.JobState, prometheus.GaugeValue, float64(jm.JobID), jm.JobState, jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.RunTime, prometheus.GaugeValue, float64(jm.JobID), jm.RunTime, jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.EligibleTime, prometheus.GaugeValue, float64(jm.JobID), jm.EligibleTime, jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.AccrueTime, prometheus.GaugeValue, float64(jm.JobID), jm.AccrueTime, jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.SuspendTime, prometheus.GaugeValue, float64(jm.JobID), jm.SuspendTime, jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.EndTime2, prometheus.GaugeValue, float64(jm.JobID), jm.EndTime2, jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.CPU_IDs, prometheus.GaugeValue, float64(jm.JobID), jm.CPU_IDs, jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.NumCPUs, prometheus.GaugeValue, float64(jm.JobID), strconv.Itoa(jm.NumCPUs), jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.NumTasks, prometheus.GaugeValue, float64(jm.JobID), strconv.Itoa(jm.NumTasks), jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.CPUSperTask, prometheus.GaugeValue, float64(jm.JobID), strconv.Itoa(jm.CPUSperTask), jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.MinMemoryCPU, prometheus.GaugeValue, float64(jm.JobID), jm.MinMemoryCPU, jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.MemoryAssigned, prometheus.GaugeValue, float64(jm.JobID), strconv.Itoa(jm.MemoryAssigned), jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.StdErr, prometheus.GaugeValue, float64(jm.JobID), jm.StdErr, jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.StdIn, prometheus.GaugeValue, float64(jm.JobID), jm.StdIn, jm.Partition, jm.JobName, jm.User, jm.Node)
		ch <- prometheus.MustNewConstMetric(cc.StdOut, prometheus.GaugeValue, float64(jm.JobID), jm.Stdout, jm.Partition, jm.JobName, jm.User, jm.Node)
	}
}
