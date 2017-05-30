package lib

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/ecs"
	"github.com/aws/aws-sdk-go/service/ecs/ecsiface"
)

type Pruner struct {
	svc     ecsiface.ECSAPI
	cluster string
}

func NewPruner(cluster string) *Pruner {
	session := session.New()
	svc := ecs.New(session)
	return &Pruner{
		svc:     svc,
		cluster: cluster,
	}
}

// GetPendingTasks returns all tasks that are in the "PENDING" state
func (p *Pruner) GetPendingTasks() ([]*ecs.Task, error) {
	// First fetch all task ARNs
	listInput := &ecs.ListTasksInput{
		Cluster: aws.String(p.cluster),
	}

	arns := []*string{}
	if err := p.svc.ListTasksPages(listInput, func(o *ecs.ListTasksOutput, lastPage bool) bool {
		arns = append(arns, o.TaskArns...)
		return !lastPage
	}); err != nil {
		return nil, err
	}

	// List doesn't give us the task status, we need to call Describe for that.
	// Furthermore, you can only describe 100 tasks at a time.

	tasks := []*ecs.Task{}
	for i := 0; i < len(arns); i += 100 {
		end := i + 100
		if end > len(arns) {
			end = len(arns)
		}
		batch := arns[i:end]

		describeInput := &ecs.DescribeTasksInput{
			Cluster: aws.String(p.cluster),
			Tasks:   batch,
		}

		describeOutput, err := p.svc.DescribeTasks(describeInput)
		if err != nil {
			return nil, err
		}
		for _, task := range describeOutput.Tasks {
			if *task.LastStatus == "PENDING" {
				tasks = append(tasks, task)
			}
		}
	}
	return tasks, nil
}

// PruneTasks takes a list of pending tasks and a time cutoff, and attempts to
// stop all tasks that have been pending since before that cutoff.  It returns
// a list of task arns that were successfully stopped.
func (p *Pruner) PruneTasks(tasks []*ecs.Task, olderThan time.Time) ([]*string, error) {
	toStop := []*ecs.Task{}

	for _, task := range tasks {
		if task.CreatedAt.Before(olderThan) {
			toStop = append(toStop, task)
		}
	}

	stopped := []*string{}
	for _, task := range toStop {
		stopInput := &ecs.StopTaskInput{
			Cluster: aws.String(p.cluster),
			Reason:  aws.String("pruned by prune-pending-tasks"),
			Task:    task.TaskArn,
		}
		stopOutput, err := p.svc.StopTask(stopInput)
		if err != nil {
			return stopped, err
		}
		stopped = append(stopped, stopOutput.Task.TaskArn)
	}
	return stopped, nil
}
