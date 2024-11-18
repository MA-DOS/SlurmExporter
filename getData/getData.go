package getData

// This package is used to get running slurm jobs by executing the squeue command and logging the output
// Func comments need to be updated
import (
	"encoding/csv"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
)

const (
	// Path of the log file
	logPath = "/home/nfomin3/dev/nextflow/chipseq/slurm_job_exporter.csv"
)

// This struct is used to store the output of the squeue command
type SlurmJob struct {
	JobID             int
	Partition         string
	Name              string
	User              string
	Time              string
	Node              string
	EnergyConsumption int
}

// var jobs []SlurmJob

// This function returns a new Job struct
func NewSlurmJob(jobID int, partition, name, user, time string /*node string,*/, joules int) SlurmJob {
	return SlurmJob{
		JobID:     jobID,
		Partition: partition,
		Name:      name,
		User:      user,
		Time:      time,
		//Node:              node,
		EnergyConsumption: joules,
	}
}

func QueryJobs() string {
	// TODO: Need to make sure queried Jobs are permanently written to file and not updated

	output, err := exec.Command("squeue").Output()
	if err != nil {
		logrus.Error("Error executing squeue command: ", err)
	}
	if err == nil {
		logrus.Info("Empty result from squeue command: ", string(output))
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
		logrus.Info("Empty result from sstat command: ", string(output))
	}
	logrus.Info("Output from sstat command: ", string(output))
	return string(output)
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

func WriteToLog(jobs []SlurmJob) {
	file, err := os.Create(logPath)
	if err != nil {
		logrus.Error("Error creating jobs.log file: ", err)
	}

	// TODO: Create Header for the log file or even write to a csv file
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	header := []string{"JobID", "Partition", "Name", "User" /*"Node",*/, "Energy Consumption"}
	if err := writer.Write(header); err != nil {
		logrus.Error("Error writing header to jobs.log file: ", err)
		return
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
			return
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
