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

// This struct is used to store the output of the squeue command
type SlurmJob struct {
	// squeue --noheader --format="%A|%j|%P|%u|%N|%c|%D|%e|%L|%m|%o|%q|%r|%T|%x|%C|%d|%B|%X|%I|%V|%Z|%S"
	JobID          int       `squeue:"%A" && scontrol:"show job --details"`
	JobName        string    `squeue:"%j"`
	Partition      string    `squeue:"%P"`
	User           string    `squeue:"%u"`
	Node           string    `squeue:"%N"`
	MinCPUs        int       `squeue:"%c"`
	MinTmpDisk     int       `squeue:"%D"`
	EndTime        time.Time `squeue:"%e"`
	TimeLimit      string    `squeue:"%L"`
	CoresPerSocket string    `squeue:"%X"`
	MinMemory      string    `squeue:"%m"`
	Command        string    `squeue:"%o"`
	Priority       string    `squeue:"%q"`
	Reason         string    `squeue:"%r"`
	State          string    `squeue:"%T"`
	SCT            string    `squeue:"%x"`
	CPUs           int       `squeue:"%C"`
	Dependency     string    `squeue:"%d"`
	SocketsPerNode string    `squeue:"%B"`
	ThreadsPerCore string    `squeue:"%I"`
	TimeLeft       string    `squeue:"%V"`
	WorkDir        string    `squeue:"%Z"`
	SubmitTime     time.Time `squeue:"%S"`
	// scontrol show job --details
	JobState       string    `scontrol:"show job --details"`
	RunTime        time.Time `scontrol:"show job --details"`
	EligibleTime   time.Time `scontrol:"show job --details"`
	AccrueTime     time.Time `scontrol:"show job --details"`
	SuspendTime    time.Time `scontrol:"show job --details"`
	EndTime2       time.Time `scontrol:"show job --details"`
	CPU_IDs        string    `scontrol:"show job --details"`
	NumCPUs        int       `scontrol:"show job --details"`
	NumTasks       int       `scontrol:"show job --details"`
	CPUSperTask    int       `scontrol:"show job --details"`
	MinMemoryCPU   string    `scontrol:"show job --details"`
	MemoryAssigned int       `scontrol:"show job --details"`
	StdErr         string    `scontrol:"show job --details"`
	StdIn          string    `scontrol:"show job --details"`
	Stdout         string    `scontrol:"show job --details"`
	// scontrol listpids -j
	// TODO: This needs a helper func to get PIDs for each job struct
	ProcessIDs []uint `scontrol:"listpids -j"`
	// sstat --format=MaxVMSize,AveVMSize,MaxRSS,AveRSS,AveCPU,AveCPUFreq,ConsumedEnergy,MaxDiskRead,MaxDiskWrite,TRESUsageOutAve
	MaxVMSize       string    `sstat:"MaxVMSize"`
	AveVMSize       string    `sstat:"AveVMSize"`
	MaxRSS          string    `sstat:"MaxRSS"`
	AveRSS          string    `sstat:"AveRSS"`
	AveCPU          time.Time `sstat:"AveCPU"`
	AveCPUFreq      string    `sstat:"AveCPUFreq"`
	ConsumedEnergy  uint32    `sstat:"ConsumedEnergy"`
	MaxDiskRead     uint32    `sstat:"MaxDiskRead"`
	MaxDiskWrite    uint32    `sstat:"MaxDiskWrite"`
	TRESUsageOutAve uint32    `sstat:"TRESUsageOutAve"`
	// sacct --format=NTasks,Cluster,NCPUS,ConsumedEnergyRaw,ConsumedEnergy,SystemCPU,TotalCPU,CPUTimeRAW,CPUTime,End,UserCPU,AllocNodes
	NTasks            uint      `sacct:"--format=NTasks"`
	Cluster           string    `sacct:"--format=Cluster"`
	NCPUS             uint      `sacct:"--format=NCPUS"`
	ConsumedEnergyRaw uint32    `sacct:"--format=ConsumedEnergyRaw"`
	ConsumedEnergy2   string    `sacct:"--format=ConsumedEnergy"`
	SystemCPU         time.Time `sacct:"--format=SystemCPU"`
	TotalCPU          time.Time `sacct:"--format=TotalCPU"`
	CPUTimeRAW        time.Time `sacct:"--format=CPUTimeRAW"`
	CPUTime           time.Time `sacct:"--format=CPUTime"`
	End               time.Time `sacct:"--format=End"`
	UserCPU           time.Time `sacct:"--format=UserCPU"`
	AllocNodes        uint      `sacct:"--format=AllocNodes"`
}

// var jobs []SlurmJob

// TODO: Modify so struct init functions are wrapped properly

func SlurmJobGetMetrics() *[]SlurmJob {
	// Init List of SlurmJobs
	jobList := map[int]SlurmJob{}
	i := 0
	for {

	}
	// Get queue metrics
	queueMetrics := ParseSlurmQueueMetrics(SlurmQueueData())

	// Get control metrics
	controlMetrics := ParseSlurmControlMetrics(SlurmControlData())

	// Map them on the JobID
	jobs
}

// TODO: It's wrong to return a struct here -> should return a map of metrics per JobID instead
func ParseSlurmQueueMetrics(input []byte) *map[int]any {
	jobsInQueue := make(map[int]any)
	lines := strings.Split(strings.TrimSpace(string(input)), "\n")
	for _, line := range lines {
		if strings.Contains(line, "|") {
			var jm SlurmJob
			splitted := strings.Split(line, "|")
			jm.JobID, _ = strconv.Atoi(splitted[0])
			jm.JobName = splitted[1]
			jm.Partition = splitted[2]
			jm.User = splitted[3]
			jm.Node = splitted[4]
			jm.MinCPUs, _ = strconv.Atoi(splitted[5])
			jm.MinTmpDisk, _ = strconv.Atoi(splitted[6])
			jm.EndTime, _ = time.Parse("2006-01-02T15:04:05", splitted[7])
			jm.TimeLimit = splitted[8]
			jm.MinMemory = splitted[9]
			jm.Command = splitted[10]
			jm.Priority = splitted[11]
			jm.Reason = splitted[12]
			jm.State = splitted[13]
			jm.SCT = splitted[14]
			jm.CPUs, _ = strconv.Atoi(splitted[15])
			jm.Dependency = splitted[16]
			jm.SocketsPerNode = splitted[17]
			jm.CoresPerSocket = splitted[18]
			jm.ThreadsPerCore = splitted[19]
			jm.TimeLeft = splitted[20]
			jm.WorkDir = splitted[21]
			jm.SubmitTime, _ = time.Parse("2006-01-02T15:04:05", splitted[22])
			jobs = append(jobs, jm)
		}
	}
	return &jobs
}

// TODO: It's wrong to return a struct here -> should return a map of metrics per JobID instead
func ParseSlurmControlMetrics(input []byte) *[]SlurmJob {
	var jobs []SlurmJob
	lines := strings.Split(strings.TrimSpace(string(input)), "\n")
	for _, line := range lines {
		if strings.Contains(line, " ") {
			splitted := strings.Split(line, " ")
			var jm SlurmJob
			for _, fields := range splitted {
				if strings.Contains(fields, "=") {
					keyValue := strings.Split(fields, "=")
					key := keyValue[0]
					value := keyValue[1]
					switch key {
					case "JobId":
						jm.JobID, _ = strconv.Atoi(value)
					case "JobState":
						jm.JobState = value
					case "RunTime":
						jm.RunTime, _ = time.Parse("2006-01-02T15:04:05", value)
					case "EligibleTime":
						jm.EligibleTime, _ = time.Parse("2006-01-02T15:04:05", value)
					case "AccrueTime":
						jm.AccrueTime, _ = time.Parse("2006-01-02T15:04:05", value)
					case "SuspendTime":
						jm.SuspendTime, _ = time.Parse("2006-01-02T15:04:05", value)
					case "EndTime":
						jm.EndTime2, _ = time.Parse("2006-01-02T15:04:05", value)
					case "NumTasks":
						jm.NumTasks, _ = strconv.Atoi(value)
					case "NumCPUs":
						jm.NumCPUs, _ = strconv.Atoi(value)
					case "Mem":
						jm.MemoryAssigned, _ = strconv.Atoi(value)
					case "MinMemoryCPU":
						jm.MinMemoryCPU = value
					case "CPUs/Task":
						jm.CPUSperTask, _ = strconv.Atoi(value)
					case "CPU_IDs":
						jm.CPU_IDs = value
					case "StdErr":
						jm.StdErr = value
					case "StdIn":
						jm.StdIn = value
					case "Stdout":
						jm.Stdout = value
					}
				}
			}
			jobs = append(jobs, jm)
		}
	}
	return &jobs
}

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

/*
// This function returns a new Job struct
func NewSlurmJob(jobID int, partition, name, user string, joules int, processIDs []int) SlurmJob {
	return SlurmJob{
		JobID:             jobID,
		Partition:         partition,
		Name:              name,
		User:              user,
		EnergyConsumption: joules,
		ProcessIDs:        processIDs,
	}
}

func QueryJobs() string {
	output, err := exec.Command("squeue").Output()
	if err != nil {
		logrus.Error("Error executing squeue command: ", err)
	}
	if err == nil {
		logrus.Info("Empty result from squeue command!")
	}
	logrus.Info("Output from squeue command: ", string(output))
	return string(output)
}

func QueryStats(job int) string {
	// Query stats for a specific job
	jobIDStr := strconv.Itoa(job)
	output, err := exec.Command("sstat", jobIDStr).Output()
	if err != nil {
		logrus.Error("Error executing sstat command: ", err)
	}
	if err == nil {
		logrus.Info("Empty result from sstat command!")
	}
	logrus.Info("Output from sstat command: ", string(output))
	return string(output)
}

func QueryJobProcesses(job int) string {
	JobIDStr := strconv.Itoa(job)
	cmd := exec.Command("scontrol", "listpids", JobIDStr)
	out, err := cmd.CombinedOutput()
	if err != nil {
		logrus.Error("Error executing scontrol listpids command: ", err)
		logrus.Error("Output from scontrol listpids command: ", string(out))
		return ""
	}
	logrus.Info("Output from scontrol listpids command: ", string(out))
	return string(out)
}

func ParseJobOutput(output string) []SlurmJob {
	// Define a regex that matches the job output of squeue
	re := regexp.MustCompile(`(\d+)\s+(\S+)\s+(\S+)\s+(\S+)\s+\S+\s+(\S+)\s+\d+\s+(\S+)`)

	var jobs []SlurmJob

	// Split the output into lines
	lines := strings.Split(output, "\n")

	for _, line := range lines {
		if line == "" {
			logrus.Info("No jobs found")
		}
		// Iterate over the lines and parse fields only if ID is not seen before
		match := re.FindStringSubmatch(line)
		if len(match) == 7 {
			jobID, _ := strconv.Atoi(match[1])
			// Query stats for the job
			QueryStats(jobID)
			QueryJobProcesses(jobID)
			if !isJobPresent(jobs, jobID) {
				job := SlurmJob{JobID: jobID, Partition: match[2], Name: match[3], User: match[4], Time: match[5]} //Node: match[6]}
				jobs = append(jobs, job)
			} else {
				continue
			}
			logrus.Info("Parsed jobs: ", jobs)
		}
	}

	for i, job := range jobs {
		// Query stats for the job
		energyOutput := QueryStats(job.JobID)
		jobs[i].EnergyConsumption = parseStats(energyOutput)
	}
	logrus.Info("Parsed jobs: ", jobs)
	return jobs
}

func parseStats(output string) (consumedEnergy int) {
	// Parse the output of sstat command
	re := regexp.MustCompile(`energy=(\d+),`)
	energy := re.FindStringSubmatch(output)
	if len(energy) == 2 {
		logrus.Info("Consumed Energy: ", energy[1])
		consumedEnergy, _ = strconv.Atoi(energy[1])
	} else {
		logrus.Info("No Consumed Energy found")
	}
	return consumedEnergy
}

func parseProcesses(output string) []int { return nil }

func WriteToLog(jobs []SlurmJob) {
	file, err := os.Create(logPath)
	if err != nil {
		logrus.Error("Error creating log file: ", err)
	}

	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	header := []string{"JobID", "Partition", "Name", "User", "Energy Consumption"}
	if err := writer.Write(header); err != nil {
		logrus.Error("Error writing header to jobs.log file: ", err)
	}

	for _, job := range jobs {
		record := []string{
			strconv.Itoa(job.JobID),
			job.Partition,
			job.Name,
			job.User,
			//job.Node,
			(strconv.Itoa(job.EnergyConsumption)),
		}
		if err := writer.Write(record); err != nil {
			logrus.Error("Error writing record to jobs.log file: ", err)
		}
	}
}

func isJobPresent(jobs []SlurmJob, jobID int) bool {
	// Check if the job is already present in the list
	for _, j := range jobs {
		if j.JobID == jobID {
			return true
		}
	}
	return false
}
*/
