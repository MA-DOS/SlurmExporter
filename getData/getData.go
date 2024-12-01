package getData

// This package is used to fetch running slurm jobs by executing the slurm commands
import (
	"io"
	"os/exec"
	"strconv"
	"strings"

	"time"

	"github.com/sirupsen/logrus"
)

/*
const (
	// Path of the log file
	logPath = "/home/nfomin3/dev/nextflow/chipseq/slurm_job_exporter.csv"
)
*/

// This struct is used to store the output of the slurm cli interface
// TODO: Name the struct fields to match the slurm cli output
type SlurmJob struct {
	// squeue --noheader --format="%A|%j|%P|%u|%N|%c|%D|%e|%L|%m|%o|%q|%r|%T|%x|%C|%d|%B|%X|%I|%V|%Z|%S"
	JobID          int    `squeue:"%A" && scontrol:"show job --details"`
	JobName        string `squeue:"%j"`
	Partition      string `squeue:"%P"`
	User           string `squeue:"%u"`
	Node           string `squeue:"%N"`
	MinCPUs        int    `squeue:"%c"`
	MinTmpDisk     int    `squeue:"%D"`
	EndTime        string `squeue:"%e"`
	TimeLimit      string `squeue:"%L"`
	CoresPerSocket string `squeue:"%X"`
	MinMemory      string `squeue:"%m"`
	Command        string `squeue:"%o"`
	Priority       string `squeue:"%q"`
	Reason         string `squeue:"%r"`
	State          string `squeue:"%T"`
	SCT            string `squeue:"%x"`
	CPUs           int    `squeue:"%C"`
	Dependency     string `squeue:"%d"`
	SocketsPerNode string `squeue:"%B"`
	ThreadsPerCore string `squeue:"%I"`
	TimeLeft       string `squeue:"%V"`
	WorkDir        string `squeue:"%Z"`
	SubmitTime     string `squeue:"%S"`
	// scontrol show job --details
	JobState       string `scontrol:"show job --details"`
	RunTime        string `scontrol:"show job --details"`
	EligibleTime   string `scontrol:"show job --details"`
	AccrueTime     string `scontrol:"show job --details"`
	SuspendTime    string `scontrol:"show job --details"`
	EndTime2       string `scontrol:"show job --details"`
	CPU_IDs        string `scontrol:"show job --details"`
	NumCPUs        int    `scontrol:"show job --details"`
	NumTasks       int    `scontrol:"show job --details"`
	CPUSperTask    int    `scontrol:"show job --details"`
	MinMemoryCPU   string `scontrol:"show job --details"`
	MemoryAssigned int    `scontrol:"show job --details"`
	StdErr         string `scontrol:"show job --details"`
	StdIn          string `scontrol:"show job --details"`
	Stdout         string `scontrol:"show job --details"`
	// scontrol listpids -j
	// TODO: This needs a helper func to get PIDs for each job struct
	ProcessIDs []uint `scontrol:"listpids -j"`
	// sstat --format=MaxVMSize,AveVMSize,MaxRSS,AveRSS,AveCPU,AveCPUFreq,ConsumedEnergy,MaxDiskRead,MaxDiskWrite,TRESUsageOutAve
	MaxVMSize       string `sstat:"MaxVMSize"`
	AveVMSize       string `sstat:"AveVMSize"`
	MaxRSS          string `sstat:"MaxRSS"`
	AveRSS          string `sstat:"AveRSS"`
	AveCPU          string `sstat:"AveCPU"`
	AveCPUFreq      string `sstat:"AveCPUFreq"`
	ConsumedEnergy  uint32 `sstat:"ConsumedEnergy"`
	MaxDiskRead     uint32 `sstat:"MaxDiskRead"`
	MaxDiskWrite    uint32 `sstat:"MaxDiskWrite"`
	TRESUsageOutAve uint32 `sstat:"TRESUsageOutAve"`
	// sacct --format=NTasks,Cluster,NCPUS,ConsumedEnergyRaw,ConsumedEnergy,SystemCPU,TotalCPU,CPUTimeRAW,CPUTime,End,UserCPU,AllocNodes
	NTasks            uint   `sacct:"--format=NTasks"`
	Cluster           string `sacct:"--format=Cluster"`
	NCPUS             uint   `sacct:"--format=NCPUS"`
	ConsumedEnergyRaw uint32 `sacct:"--format=ConsumedEnergyRaw"`
	ConsumedEnergy2   string `sacct:"--format=ConsumedEnergy"`
	SystemCPU         string `sacct:"--format=SystemCPU"`
	TotalCPU          string `sacct:"--format=TotalCPU"`
	CPUTimeRAW        string `sacct:"--format=CPUTimeRAW"`
	CPUTime           string `sacct:"--format=CPUTime"`
	End               string `sacct:"--format=End"`
	UserCPU           string `sacct:"--format=UserCPU"`
	AllocNodes        uint   `sacct:"--format=AllocNodes"`
}

// TODO: Refactor to not switch case on all metrics but on the struct fields variables
func (sjs *SlurmJob) NewSlurmJobStruct(metrics map[any]interface{}) *[]SlurmJob {
	var slurmJobs []SlurmJob
	// Iterate over each jobID in the map and init the struct with the nested map metrics
	for jobID := range metrics {
		logrus.Info("Initializing struct for jobID: ", jobID)
		sjs.JobID = jobID.(int)
		for k, v := range metrics[jobID].(map[any]interface{}) {
			switch k {
			case "JobName":
				sjs.JobName = v.(string)
			case "Partition":
				sjs.Partition = v.(string)
			case "User":
				sjs.User = v.(string)
			case "Node":
				sjs.Node = v.(string)
			case "MinCPUs":
				sjs.MinCPUs, _ = strconv.Atoi(v.(string))
			case "MinTmpDisk":
				sjs.MinTmpDisk, _ = strconv.Atoi(v.(string))
			case "EndTime":
				parsedTime, _ := time.Parse("15:04:05", v.(string))
				sjs.EndTime = parsedTime.Format("15:04:05")
			case "TimeLimit":
				sjs.TimeLimit = v.(string)
			case "CoresPerSocket":
				sjs.CoresPerSocket = v.(string)
			case "MinMemory":
				sjs.MinMemory = v.(string)
			case "Command":
				sjs.Command = v.(string)
			case "Priority":
				sjs.Priority = v.(string)
			case "Reason":
				sjs.Reason = v.(string)
			case "State":
				sjs.State = v.(string)
			case "SCT":
				sjs.SCT = v.(string)
			case "CPUs":
				sjs.CPUs, _ = strconv.Atoi(v.(string))
			case "Dependency":
				sjs.Dependency = v.(string)
			case "SocketsPerNode":
				sjs.SocketsPerNode = v.(string)
			case "ThreadsPerCore":
				sjs.ThreadsPerCore = v.(string)
			case "TimeLeft":
				sjs.TimeLeft = v.(string)
			case "WorkDir":
				sjs.WorkDir = v.(string)
			case "SubmitTime":
				parsedTime, _ := time.Parse("15:04:05", v.(string))
				sjs.SubmitTime = parsedTime.Format("15:04:05")
			case "JobState":
				sjs.JobState = v.(string)
			case "RunTime":
				parsedTime, _ := time.Parse("15:04:05", v.(string))
				sjs.RunTime = parsedTime.Format("15:04:05")
			case "EligibleTime":
				parsedTime, _ := time.Parse("15:04:05", v.(string))
				sjs.EligibleTime = parsedTime.Format("15:04:05")
			case "AccrueTime":
				parsedTime, _ := time.Parse("15:04:05", v.(string))
				sjs.AccrueTime = parsedTime.Format("15:04:05")
			case "SuspendTime":
				parsedTime, _ := time.Parse("15:04:05", v.(string))
				sjs.SuspendTime = parsedTime.Format("15:04:05")
			case "EndTime2":
				parsedTime, _ := time.Parse("15:04:05", v.(string))
				sjs.EndTime2 = parsedTime.Format("15:04:05")
			case "CPU_IDs":
				sjs.CPU_IDs = v.(string)
			case "NumCPUs":
				sjs.NumCPUs, _ = strconv.Atoi(v.(string))
			case "NumTasks":
				sjs.NumTasks, _ = strconv.Atoi(v.(string))
			case "CPUSperTask":
				sjs.CPUSperTask, _ = strconv.Atoi(v.(string))
			case "MinMemoryCPU":
				sjs.MinMemoryCPU = v.(string)
			case "MemoryAssigned":
				sjs.MemoryAssigned, _ = strconv.Atoi(v.(string))
			case "StdErr":
				sjs.StdErr = v.(string)
			case "StdIn":
				sjs.StdIn = v.(string)
			case "Stdout":
				sjs.Stdout = v.(string)
			}
			slurmJobs = append(slurmJobs, *sjs)
		}
	}
	return &slurmJobs
}

// TODO: Add a func call to get sstat metrics and parse the results directly into the tmpMetricsMaps
func AggregateSlurmMetrics(metricMaps ...map[any]interface{}) map[any]interface{} {
	// Read in the maps containing the metrics
	queueMetrics := metricMaps[0]
	controlMetrics := metricMaps[1]
	mergedMetricsMap := make(map[any]interface{})

	for queueKey := range queueMetrics {
		if _, exists := controlMetrics[queueKey]; exists {
			// Merge the metrics into one map
			tmpMetricsMap := make(map[any]interface{})
			for k, v := range queueMetrics[queueKey].(map[any]interface{}) {
				tmpMetricsMap[k] = v
			}
			for k, v := range controlMetrics[queueKey].(map[any]interface{}) {
				tmpMetricsMap[k] = v
			}
			mergedMetricsMap[queueKey] = tmpMetricsMap
			// Call func to init the struct with metrics from both maps
		} else {
			logrus.Info("No match found for jobID: ", queueKey)
		}
	}
	return mergedMetricsMap
}

func ParseSlurmControlMetrics(input []byte) map[any]interface{} {
	tokens := strings.Fields(string(input))
	scontrolMap := make(map[any]interface{})
	var jobID int

	for _, token := range tokens {
		keyValue := strings.SplitN(token, "=", 2)
		if len(keyValue) != 2 {
			continue
		}
		key, value := keyValue[0], keyValue[1]
		if key == "JobId" {
			jobID, _ = (strconv.Atoi(value))
			metrics := make(map[any]interface{})
			scontrolMap[jobID] = metrics
			//scontrolMap[jobID] = make(map[any]any)
		} else if jobID != 0 {
			metrics := scontrolMap[jobID].(map[any]interface{})
			//scontrolMap[jobID][key] = value
			metrics[key] = value
			scontrolMap[jobID] = metrics
		}
	}
	return scontrolMap
}

// TODO: Maybe shorten to same logic as control parser
func ParseSlurmQueueMetrics(input []byte) map[any]interface{} {
	jobsInQueue := make(map[any]interface{})
	lines := strings.Split(strings.TrimSpace(string(input)), "\n")
	for _, line := range lines {
		if strings.Contains(line, "|") {
			splitted := strings.Split(line, "|")
			jobID, _ := strconv.Atoi(splitted[0])
			metrics := map[any]interface{}{
				"JobName":        splitted[1],
				"Partition":      splitted[2],
				"User":           splitted[3],
				"Node":           splitted[4],
				"MinCPUs":        splitted[5],
				"MinTmpDisk":     splitted[6],
				"EndTime":        splitted[7],
				"TimeLimit":      splitted[8],
				"MinMemory":      splitted[9],
				"Command":        splitted[10],
				"Priority":       splitted[11],
				"Reason":         splitted[12],
				"State":          splitted[13],
				"SCT":            splitted[14],
				"CPUs":           splitted[15],
				"Dependency":     splitted[16],
				"SocketsPerNode": splitted[17],
				"CoresPerSocket": splitted[18],
				"ThreadsPerCore": splitted[19],
				"TimeLeft":       splitted[20],
				"WorkDir":        splitted[21],
				"SubmitTime":     splitted[22],
			}
			jobsInQueue[jobID] = metrics
		}
	}
	return jobsInQueue
}

// TODO: Needs to be implemented
func ParseSlurmSacctMetrics(input []byte) *[]SlurmJob { return nil }

func SlurmQueueData() []byte {
	cmd := exec.Command("squeue", "--noheader", "--format=%A|%j|%P|%u|%N|%c|%D|%e|%L|%m|%o|%q|%r|%T|%x|%C|%d|%B|%X|%I|%V|%Z|%S")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		logrus.Error("Error creating stdout pipe: ", err)
	}
	if err := cmd.Start(); err != nil {
		logrus.Error("Error starting squeue command: ", err)
	}
	out, _ := io.ReadAll(stdout)
	if err := cmd.Wait(); err != nil {
		logrus.Error("Error waiting for squeue command: ", err)
	}
	return out
}

func SlurmControlData() []byte {
	cmd := exec.Command("scontrol", "show", "job", "--details")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		logrus.Error("Error creating stdout pipe: ", err)
	}
	if err := cmd.Start(); err != nil {
		logrus.Error("Error starting scontrol command: ", err)
	}
	out, _ := io.ReadAll(stdout)
	if err := cmd.Wait(); err != nil {
		logrus.Error("Error waiting for scontrol command: ", err)
	}
	return out
}

// TODO: Add -j flag to command
func SlurmStatData() []byte {
	cmd := exec.Command("sstat", "--format=MaxVMSize,AveVMSize,MaxRSS,AveRSS,AveCPU,AveCPUFreq,ConsumedEnergy,MaxDiskRead,MaxDiskWrite,TRESUsageOutAve")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		logrus.Error("Error creating stdout pipe: ", err)
	}
	if err := cmd.Start(); err != nil {
		logrus.Error("Error starting sstat command: ", err)
	}
	out, _ := io.ReadAll(stdout)
	if err := cmd.Wait(); err != nil {
		logrus.Error("Error waiting for sstat command: ", err)
	}
	return out
}

// TODO: Add timestamps to shorten to current workflow run
func SlurmSacctData() []byte {
	cmd := exec.Command("sacct", "--format=NTasks,Cluster,NCPUS,ConsumedEnergyRaw,ConsumedEnergy,SystemCPU,TotalCPU,CPUTimeRAW,CPUTime,End,UserCPU,AllocNodes")
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		logrus.Error("Error creating stdout pipe: ", err)
	}
	if err := cmd.Start(); err != nil {
		logrus.Error("Error starting sacct command: ", err)
	}
	out, _ := io.ReadAll(stdout)
	if err := cmd.Wait(); err != nil {
		logrus.Error("Error waiting for sacct command: ", err)
	}
	return out
}
