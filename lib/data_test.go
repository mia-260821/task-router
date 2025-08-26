package lib

import (
	"github.com/stretchr/testify/assert"
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

	block, ok := Serialize[Task](task1)
	assert.True(t, ok, "serialize task")
	assert.NotNil(t, block, "serialize block")

	deTask1, ok := Deserialize[Task](block)
	assert.True(t, ok, "deserialize task")
	assert.Equal(t, task1, deTask1, "deserialize task")
}
