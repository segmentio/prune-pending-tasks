package lib

import (
	"errors"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/ecs"
	"github.com/aws/aws-sdk-go/service/ecs/ecsiface"
	"github.com/stretchr/testify/assert"
)

func NewTestPruner(tasks []*ecs.Task) *Pruner {
	return &Pruner{
		svc: &MockECSAPI{Tasks: tasks},
	}
}

type MockECSAPI struct {
	ecsiface.ECSAPI
	Tasks []*ecs.Task
}

func (m *MockECSAPI) ListTasksPages(input *ecs.ListTasksInput, iterator func(o *ecs.ListTasksOutput, lastPage bool) bool) error {
	arns := []*string{}
	for _, task := range m.Tasks {
		arns = append(arns, task.TaskArn)
	}
	output := &ecs.ListTasksOutput{
		NextToken: nil,
		TaskArns:  arns,
	}
	iterator(output, true)
	return nil
}

func (m *MockECSAPI) DescribeTasks(input *ecs.DescribeTasksInput) (*ecs.DescribeTasksOutput, error) {
	return &ecs.DescribeTasksOutput{
		Tasks: m.Tasks,
	}, nil
}

func (m *MockECSAPI) StopTask(input *ecs.StopTaskInput) (*ecs.StopTaskOutput, error) {
	for _, task := range m.Tasks {
		if task.TaskArn == input.Task {
			return &ecs.StopTaskOutput{
				Task: task,
			}, nil
		}
	}
	return &ecs.StopTaskOutput{}, errors.New("couldnt find task")
}

func TestGetPendingTasks(t *testing.T) {
	testPendingTasks := []*ecs.Task{
		{
			LastStatus: aws.String("PENDING"),
			CreatedAt:  aws.Time(time.Now()),
			TaskArn:    aws.String("just-started"),
		},
		{
			LastStatus: aws.String("PENDING"),
			CreatedAt:  aws.Time(time.Now().Add(-24 * time.Hour)),
			TaskArn:    aws.String("started-yesterday"),
		},
	}

	testOtherTasks := []*ecs.Task{
		{
			LastStatus: aws.String("RUNNING"),
			CreatedAt:  aws.Time(time.Now()),
			TaskArn:    aws.String("running"),
		},
		{
			LastStatus: aws.String("STOPPED"),
			CreatedAt:  aws.Time(time.Now()),
			TaskArn:    aws.String("stopped"),
		},
	}

	allTasks := []*ecs.Task{}
	allTasks = append(allTasks, testPendingTasks...)
	allTasks = append(allTasks, testOtherTasks...)

	pruner := NewTestPruner(allTasks)

	pending, err := pruner.GetPendingTasks()
	assert.Nil(t, err)
	assert.Equal(t, testPendingTasks, pending)
}

func TestPruneTasks(t *testing.T) {
	testPendingTasks := []*ecs.Task{
		{
			LastStatus: aws.String("PENDING"),
			CreatedAt:  aws.Time(time.Now()),
			TaskArn:    aws.String("just-started"),
		},
		{
			LastStatus: aws.String("PENDING"),
			CreatedAt:  aws.Time(time.Now().Add(-24 * time.Hour)),
			TaskArn:    aws.String("started-yesterday"),
		},
	}

	pruner := NewTestPruner(testPendingTasks)

	stopped, err := pruner.PruneTasks(testPendingTasks, time.Now().Add(-time.Hour))
	assert.Nil(t, err)
	assert.Equal(t, 1, len(stopped))
	assert.Equal(t, "started-yesterday", *stopped[0])
}
