package lib

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"task-router/lib/utils"
	"testing"
)

func TestTask(t *testing.T) {

	task1 := Task{
		Id: "1",
		Meta: TaskMeta{
			OS:   "linux",
			Arch: "amd64",
		},
		Command: TaskCommand{
			Name: "echo",
			Args: []string{"hello world"},
		},
	}

	block, ok := utils.Serialize[Task](task1)
	assert.True(t, ok, "serialize task")
	assert.NotNil(t, block, "serialize block")

	deTask1, ok := utils.Deserialize[Task](block)
	assert.True(t, ok, "deserialize task")
	assert.Equal(t, task1, deTask1, "deserialize task")

	task2 := Task{
		Id:   "2",
		Meta: TaskMeta{OS: "windows"},
		Command: TaskCommand{
			Name: "echo", Args: []string{"hi"},
		},
	}

	tasks := []Task{task1, task2}

	block, ok = utils.Serialize(tasks)
	assert.True(t, ok, "serialize tasks")
	assert.NotNil(t, block, "serialize block")

	deTasks, ok := utils.Deserialize[[]Task](block)
	assert.True(t, ok, "deserialize task")
	assert.Equal(t, tasks, deTasks, "deserialize task")

	fmt.Printf("%+v\n", deTasks)
}
