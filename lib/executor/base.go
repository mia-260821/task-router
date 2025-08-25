package executor

import (
	"context"
	"os"
	"os/exec"
)

const (
	STDIN  = "stdin"
	STDOUT = "stdin"
	STDERR = "stderr"
)

type Task interface {
	Name() string
	Args() []string
	Context() context.Context
}

type Executor interface {
	Run(task Task) error
}

type BaseExecutor struct{}

func (e *BaseExecutor) Run(task Task) error {
	ctx := task.Context()

	cmd := exec.CommandContext(ctx, task.Name(), task.Args()...)
	var ok bool
	if cmd.Stdin, ok = ctx.Value(STDIN).(*os.File); !ok {
		cmd.Stdin = os.Stdin
	}
	if cmd.Stdout, ok = ctx.Value(STDOUT).(*os.File); !ok {
		cmd.Stdout = os.Stdout
	}
	if cmd.Stderr, ok = ctx.Value(STDERR).(*os.File); !ok {
		cmd.Stderr = os.Stderr
	}
	return cmd.Run()
}
