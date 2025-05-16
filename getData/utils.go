package getData

import (
	"io"
	"os/exec"
	"strconv"
	"strings"
	"unicode"

	"github.com/sirupsen/logrus"
)

// Helpers for the Aggregation of Slurm Metrics
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

// Helpers for Parsing the Slurm Command Outputs.
func ParseSlurmStatMetrics(input []byte, jobID int) map[any]interface{} {
	jobPIDMap := ParseSlurmJobPid(GetJobPid(jobID))
	parentJobPID, exists := jobPIDMap[jobID]
	if !exists {
		parentJobPID = 0
	}

	jobsInStats := make(map[any]interface{})

	// Jobs stats are being found. Map is created.
	if strings.Contains(string(input), "|") {
		splitted := strings.Split(string(input), "|")
		for i := range splitted {
			if splitted[i] == "" {
				splitted[i] = "0"
				// fmt.Println("Result with empty field is: ", splitted)
				// logrus.Info("Sstat returned empty fields due to suspended or completed jobs.")
			}
		}

		metrics := map[any]interface{}{
			"MaxVMSize":      TrimMetricValues(splitted[0]),
			"AveVMSize":      TrimMetricValues(splitted[1]),
			"MaxRSS":         TrimMetricValues(splitted[2]),
			"AveRSS":         TrimMetricValues(splitted[3]),
			"ConsumedEnergy": TrimMetricValues(splitted[6]),
			"MaxDiskRead":    TrimMetricValues(splitted[7]),
			"MaxDiskWrite":   TrimMetricValues(splitted[8]),
			"ParentJobPID":   parentJobPID,
		}
		// logrus.Infof("Sstat returned for job %d: %s", jobID, metrics)
		jobsInStats[jobID] = metrics
	} else /* strings.Contains(string(input), "sstat: error: couldn't get steps for job") */ {
		// logrus.Warnf("No stat metrics available for job %d. Defaulting metrics to zero.", jobID)
		metrics := map[any]interface{}{
			"MaxVMSize":      "0",
			"AveVMSize":      "0",
			"MaxRSS":         "0",
			"AveRSS":         "0",
			"ConsumedEnergy": "0",
			"MaxDiskRead":    "0",
			"MaxDiskWrite":   "0",
			"ParentJobPID":   0,
		}
		// logrus.Info("Default Stats are: ", metrics)
		jobsInStats[jobID] = metrics

	}
	return jobsInStats
}

// Helper to rm units behind metrics coming from slurm output.
func TrimMetricValues(value string) string {
	if len(value) > 0 && unicode.IsLetter(rune(value[len(value)-1])) {
		return value[:len(value)-1]
	}
	return value
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
			// scontrolMap = FilterControlMetrics(scontrolMap)
		}
	}
	// Filter out every previous jobID from the map.
	scontrolMap = RemoveCompletedJobsFromControlOuptut(RemoveOldJobsFromControlOutput(scontrolMap))
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

// Helpers for getting Slurm Parent Jobs and Parsing their PIDs.
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

func GetJobPid(jobID int) ([]byte, int) {
	cmd := exec.Command("scontrol", "listpid", strconv.Itoa(jobID))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		logrus.Error("Error creating stdout pipe: ", err)
	}
	if err := cmd.Start(); err != nil {
		logrus.Error("Error starting scontrol command: ", err)
	}
	out, _ := io.ReadAll(stdout)
	return out, jobID
}

func ParseSlurmJobPid(input []byte, jobID int) map[int]int {
	pids := make(map[int]int)
	lines := strings.Split(strings.TrimSpace(string(input)), "\n")

	if len(lines) == 0 {
		return pids
	}

	validPIDs := []int{}

	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}

		pid, err := strconv.Atoi(fields[0])
		if err != nil {
			continue
		}

		validPIDs = append(validPIDs, pid)
	}

	// Check if there are at least two PIDs
	if len(validPIDs) >= 2 {
		pids[jobID] = validPIDs[1] // Use the child's PID
	} else if len(validPIDs) == 1 {
		pids[jobID] = validPIDs[0]
	}
	return pids
}

// Helpers for Filtering the Output of SlurmControl.
func RemoveOldJobsFromControlOutput(controlMap map[any]interface{}) map[any]interface{} {
	filteredMap := make(map[any]interface{})

	for jobID, metrics := range controlMap {
		if id, ok := jobID.(int); ok {
			if id >= parentJob {
				filteredMap[jobID] = metrics
			}
		}
	}
	return filteredMap
}

func RemoveCompletedJobsFromControlOuptut(filteredControlMap map[any]interface{}) map[any]interface{} {
	duplicateFreeMap := make(map[any]interface{})

	for jobID, metrics := range filteredControlMap {
		if jobMetrics, ok := metrics.(map[any]interface{}); ok {
			if jobState, exists := jobMetrics["JobState"]; exists && jobState == "COMPLETED" && jobState != "PENDING" && jobState != "SUSPENDED" {
				if _, alreadyCompleted := completedJobs[jobID.(int)]; alreadyCompleted {
					continue
				} else {
					completedJobs[jobID.(int)] = true
					duplicateFreeMap[jobID] = metrics
					continue
				}
			} else if jobState != "PENDING" && jobState != "SUSPENDED" {
				duplicateFreeMap[jobID] = metrics
			}
		}
	}
	return duplicateFreeMap
}
