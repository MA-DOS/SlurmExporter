package getData

// This package is used to fetch running slurm jobs by executing the slurm commands
import (
	"fmt"
	"io"
	"os/exec"
	"strconv"
	"strings"

	"time"

	"github.com/sirupsen/logrus"
)

var noWorkflowRunning bool
var workflowRunning bool

// This struct is used to store the output of the slurm cli interface
type SlurmJob struct {
	// squeue --noheader --format="%A|%j|%P|%u|%N|%c|%D|%e|%L|%m|%o|%q|%r|%T|%x|%C|%d|%B|%X|%I|%V|%Z|%S"
	JobID      int    `squeue:"%A" && scontrol:"show job --details"`
	JobName    string `squeue:"%j"`
	Partition  string `squeue:"%P"`
	User       string `squeue:"%u"`
	Node       string `squeue:"%N"`
	MinCPUs    int    `squeue:"%c"`
	MinTmpDisk int    `squeue:"%D"`
	EndTime    string `squeue:"%e"`
	// EndTime        string `squeue:"%e"`
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
	// TimeLeft       string `squeue:"%V"`
	TimeLeft string `squeue:"%V"`
	WorkDir  string `squeue:"%Z"`
	// SubmitTime     string `squeue:"%S"`
	SubmitTime string `squeue:"%S"`
	// scontrol show job --details
	JobState string `scontrol:"show job --details"`
	RunTime  string `scontrol:"show job --details"`
	// EligibleTime string `scontrol:"show job --details"`
	EligibleTime string `scontrol:"show job --details"`
	AccrueTime   string `scontrol:"show job --details"`
	// SuspendTime  string `scontrol:"show job --details"`
	SuspendTime  string `scontrol:"show job --details"`
	EndTime2     string `scontrol:"show job --details"`
	CPU_IDs      string `scontrol:"show job --details"`
	NumCPUs      int    `scontrol:"show job --details"`
	NumTasks     int    `scontrol:"show job --details"`
	CPUSperTask  int    `scontrol:"show job --details"`
	MinMemoryCPU string `scontrol:"show job --details"`
	Mem          string `scontrol:"show job --details"`
	StdErr       string `scontrol:"show job --details"`
	StdIn        string `scontrol:"show job --details"`
	Stdout       string `scontrol:"show job --details"`
	// scontrol listpids -j
	// TODO: This needs a helper func to get PIDs for each job struct
	ParentJobPID int `scontrol:"listpid "`
	// sstat --format=MaxVMSize,AveVMSize,MaxRSS,AveRSS,AveCPU,AveCPUFreq,ConsumedEnergy,MaxDiskRead,MaxDiskWrite,TRESUsageOutAve
	MaxVMSize      string `sstat:"MaxVMSize"`
	AveVMSize      string `sstat:"AveVMSize"`
	MaxRSS         string `sstat:"MaxRSS"`
	AveRSS         string `sstat:"AveRSS"`
	AveCPU         string `sstat:"AveCPU"`
	AveCPUFreq     string `sstat:"AveCPUFreq"`
	ConsumedEnergy string `sstat:"ConsumedEnergy"`
	MaxDiskRead    int    `sstat:"MaxDiskRead"`
	MaxDiskWrite   int    `sstat:"MaxDiskWrite"`
	// TRESUsageOutAve int    `sstat:"TRESUsageOutAve"`
	// sacct --format=NTasks,Cluster,NCPUS,ConsumedEnergyRaw,ConsumedEnergy,SystemCPU,TotalCPU,CPUTimeRAW,CPUTime,End,UserCPU,AllocNodes
	NTasks            int    `sacct:"--format=NTasks"`
	Cluster           string `sacct:"--format=Cluster"`
	NCPUS             int    `sacct:"--format=NCPUS"`
	ConsumedEnergyRaw int    `sacct:"--format=ConsumedEnergyRaw"`
	//ConsumedEnergy2   string `sacct:"--format=ConsumedEnergy"`
	SystemCPU  string `sacct:"--format=SystemCPU"`
	TotalCPU   string `sacct:"--format=TotalCPU"`
	CPUTimeRAW string `sacct:"--format=CPUTimeRAW"`
	CPUTime    string `sacct:"--format=CPUTime"`
	End        string `sacct:"--format=End"`
	UserCPU    string `sacct:"--format=UserCPU"`
	AllocNodes int    `sacct:"--format=AllocNodes"`
}

// Init function to create a new SlurmJob struct
func SlurmJobsGetMetrics(parentJob int) *[]SlurmJob {
	qm := ParseSlurmQueueMetrics(SlurmQueueData())
	cm := ParseSlurmControlMetrics(SlurmControlData())
	metricMap := AggregateSlurmMetrics(ParseSlurmParentJob(GetSlurmParentJob()), qm, cm)

	var slurmJob SlurmJob
	result := slurmJob.NewSlurmJobStruct(parentJob, metricMap)

	return result
}

func (sjs *SlurmJob) NewSlurmJobStruct(parentJob int, metrics map[any]interface{}) *[]SlurmJob {
	var slurmJobs []SlurmJob
	if metrics == nil {
		if !noWorkflowRunning {
			logrus.Error("No metrics found... Check if Workflow is running!")
			noWorkflowRunning = true
		}
		return &slurmJobs

	} else if len(metrics) == 0 {
		if !noWorkflowRunning {
			logrus.Error("No metrics found... Check if Workflow is running!")
			noWorkflowRunning = true
		}
		return &slurmJobs
	}

	// Iterate over each jobID in the map and init the struct with the nested map metrics
	for jobID := range metrics {
		if !workflowRunning {
			logrus.Info("Metrics found... being logged to Prometheus!")
			workflowRunning = true
		}

		sjs.JobID = jobID.(int)
		// Filter out old jobs of previous workflow runs
		if sjs.JobID >= parentJob {
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
					fmt.Println(sjs.EndTime)
				case "TimeLimit":
					sjs.TimeLimit = v.(string)
					fmt.Println(sjs.TimeLimit)
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
					fmt.Println(sjs.TimeLeft)
				case "WorkDir":
					sjs.WorkDir = v.(string)
				case "SubmitTime":
					parsedTime, _ := time.Parse("15:04:05", v.(string))
					sjs.SubmitTime = parsedTime.Format("15:04:05")
					fmt.Println(sjs.SubmitTime)
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
					fmt.Println(sjs.SuspendTime)
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
				case "Mem":
					sjs.Mem, _ = v.(string)
				case "StdErr":
					sjs.StdErr = v.(string)
				case "StdIn":
					sjs.StdIn = v.(string)
				case "Stdout":
					sjs.Stdout = v.(string)
				case "MaxVMSize":
					sjs.MaxVMSize = v.(string)
				case "AveVMSize":
					sjs.AveVMSize = v.(string)
				case "MaxRSS":
					sjs.MaxRSS = v.(string)
				case "AveRSS":
					sjs.AveRSS = v.(string)
				case "AveCPU":
					sjs.AveCPU = v.(string)
				case "AveCPUFreq":
					sjs.AveCPUFreq = v.(string)
				case "ConsumedEnergy":
					sjs.ConsumedEnergy = v.(string)
				case "MaxDiskRead":
					sjs.MaxDiskRead, _ = strconv.Atoi(v.(string))
				case "MaxDiskWrite":
					sjs.MaxDiskWrite, _ = strconv.Atoi(v.(string))
				case "ParentJobPID":
					sjs.ParentJobPID, _ = v.(int)
				case "NTasks":
					sjs.NTasks, _ = strconv.Atoi(v.(string))
				case "Cluster":
					sjs.Cluster = v.(string)
				case "NCPUS":
					sjs.NCPUS, _ = strconv.Atoi(v.(string))
				case "ConsumedEnergyRaw":
					sjs.ConsumedEnergyRaw, _ = strconv.Atoi(v.(string))
				case "SystemCPU":
					sjs.SystemCPU = v.(string)
				case "TotalCPU":
					sjs.TotalCPU = v.(string)
				case "CPUTimeRAW":
					sjs.CPUTimeRAW = v.(string)
				case "CPUTime":
					sjs.CPUTime = v.(string)
				case "End":
					sjs.End = v.(string)
				case "UserCPU":
					sjs.UserCPU = v.(string)
				case "AllocNodes":
					sjs.AllocNodes, _ = strconv.Atoi(v.(string))
				}
			}
		} else {
			break
		}
		slurmJobs = append(slurmJobs, *sjs)
	}
	return &slurmJobs
}

func mapStatData(jobID any) (map[any]interface{}, error) {
	statData, job_id, err := SlurmStatData(jobID.(int))
	if err != nil {
		return nil, err
	}
	return ParseSlurmStatMetrics(statData, job_id), nil
}

func mapAcctData(parentJob int) (map[any]interface{}, error) {
	acctData, err := SlurmSacctData(parentJob)
	if err != nil {
		return nil, err
	}
	return ParseSlurmSacctMetrics(acctData), nil

}

func AggregateSlurmMetrics(parentJob int, metricMaps ...map[any]interface{}) map[any]interface{} {
	// Read in the maps containing the metrics
	queueMetrics := metricMaps[0]
	controlMetrics := metricMaps[1]
	mergedMetricsMap := make(map[any]interface{})

	// In the end every job is displayed in COMPLETED state
	for controlKey := range controlMetrics {
		if _, exists := queueMetrics[controlKey]; exists {
			tmpMetricsMap := make(map[any]interface{})

			// Write all the metrics from squeue
			for k, v := range queueMetrics[controlKey].(map[any]interface{}) {
				tmpMetricsMap[k] = v
			}

			// Write all the metrics from scontrol
			for k, v := range controlMetrics[controlKey].(map[any]interface{}) {
				tmpMetricsMap[k] = v
			}

			statMetrics, err := mapStatData(controlKey)
			if err != nil || statMetrics == nil || len(statMetrics) == 0 {
			} else {
				for k, v := range statMetrics[controlKey].(map[any]interface{}) {
					tmpMetricsMap[k] = v
				}
			}

			// acctMetrics, err := mapAcctData(parentJob)
			// if err != nil || acctMetrics == nil || len(acctMetrics) == 0 {
			// } else {
			// 	if metrics, ok := acctMetrics[controlKey].(map[any]interface{}); ok {
			// 		for k, v := range metrics {
			// 			tmpMetricsMap[k] = v
			// 		}
			// 	}
			// }

			mergedMetricsMap[controlKey] = tmpMetricsMap

		} else {
			mergedMetricsMap = controlMetrics
		}
	}
	return mergedMetricsMap
}

func ParseSlurmStatMetrics(input []byte, jobID int) map[any]interface{} {
	jobPIDMap := ParseSlurmJobPid(GetJobPid(jobID))
	parentJobPID := jobPIDMap[jobID]
	jobsInStats := make(map[any]interface{})
	if strings.Contains(string(input), "|") {
		splitted := strings.Split(string(input), "|")
		metrics := map[any]interface{}{
			"MaxVMSize":      splitted[0],
			"AveVMSize":      splitted[1],
			"MaxRSS":         splitted[2],
			"AveRSS":         splitted[3],
			"AveCPU":         splitted[4],
			"AveCPUFreq":     splitted[5],
			"ConsumedEnergy": splitted[6],
			"MaxDiskRead":    splitted[7],
			"MaxDiskWrite":   splitted[8],
			"ParentJobPID":   parentJobPID,
		}
		jobsInStats[jobID] = metrics
	}
	return jobsInStats
}

func getValueOrDefault(splitted []string, index int) string {
	if index < len(splitted) && splitted[index] != "" {
		return splitted[index]
	}
	return "0"
}

func GetJobPid(jobID int) ([]byte, int) {
	cmd := exec.Command("scontrol", "listpid", strconv.Itoa(jobID))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		logrus.Error("Error creating stdout pipe: ", err)
	}
	if err := cmd.Start(); err != nil {
		logrus.Error("Error starting sstat command: ", err)
	}
	out, _ := io.ReadAll(stdout)
	if err := cmd.Wait(); err != nil {
		logrus.Error("Job is already finished and metrics will be defaulted to zero.", err)
	}
	return out, jobID
}

func ParseSlurmJobPid(input []byte, jobID int) map[int]int {
	pids := make(map[int]int)
	lines := strings.Split(strings.TrimSpace(string(input)), "\n")

	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}

		pid, err := strconv.Atoi(fields[0])
		if err != nil {
			continue
		}

		localID := fields[3]
		globaldID := fields[4]

		if localID == "0" && globaldID == "0" {
			pids[jobID] = pid
		}

	}
	fmt.Printf("PIDS: %v\n", pids)
	return pids
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
		} else if jobID != 0 {
			metrics := scontrolMap[jobID].(map[any]interface{})
			metrics[key] = value
			scontrolMap[jobID] = metrics
		}
	}
	return scontrolMap
}

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
			fmt.Println(jobsInQueue)
		}
	}
	return jobsInQueue
}

func ParseSlurmSacctMetrics(input []byte) map[any]interface{} {
	jobsInAccounting := make(map[any]interface{})
	lines := strings.Split(strings.TrimSpace(string(input)), "\n")
	for _, line := range lines {
		if strings.Contains(line, "|") {
			splitted := strings.Split(line, "|")
			jobID, _ := strconv.Atoi(splitted[0])
			metrics := map[any]interface{}{
				"NTasks":            splitted[1],
				"Cluster":           splitted[2],
				"NCPUS":             splitted[3],
				"ConsumedEnergyRaw": splitted[4],
				"ConsumedEnergy":    splitted[5],
				"SystemCPU":         splitted[6],
				"TotalCPU":          splitted[7],
				"CPUTimeRAW":        splitted[8],
				"CPUTime":           splitted[9],
				"End":               splitted[10],
				"UserCPU":           splitted[11],
				"AllocNodes":        splitted[12],
			}
			jobsInAccounting[jobID] = metrics
		}
	}
	return jobsInAccounting
}

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

func SlurmStatData(jobID int) ([]byte, int, error) {
	cmd := exec.Command("sstat", "--noheader", "--format=MaxVMSize,AveVMSize,MaxRSS,AveRSS,AveCPU,AveCPUFreq,ConsumedEnergy,MaxDiskRead,MaxDiskWrite", "-p", "-j", strconv.Itoa(jobID))
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
	return out, jobID, err
}

// TODO: Add timestamps to shorten to current workflow run
func SlurmSacctData(jobID int) ([]byte, error) {
	cmd := exec.Command("bash", "-c", fmt.Sprintf("sacct --noheader --format=JobID,NTasks,Cluster,NCPUS,ConsumedEnergyRaw,ConsumedEnergy,SystemCPU,TotalCPU,CPUTimeRAW,CPUTime,End,UserCPU,AllocNodes -p | awk -F'|' '$1 >= %d' | grep -v '\\.batch'", jobID))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		logrus.Error("Error creating stdout pipe: ", err)
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		logrus.Error("Error starting sacct command: ", err)
		return nil, err
	}
	out, _ := io.ReadAll(stdout)
	return out, nil
}

func GetSlurmParentJob() []byte {
	cmd := exec.Command("squeue", "--noheader", "--format=%A")
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

func ParseSlurmParentJob(input []byte) int {
	lines := strings.Split(strings.TrimSpace(string(input)), "\n")
	if len(lines) > 0 {
		jobID, _ := strconv.Atoi(lines[0])
		return jobID
	}
	return 0
}
