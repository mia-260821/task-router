package lib

type TaskMeta struct {
	OS   string
	Arch string
}

type TaskCommand struct {
	Name string
	Args []string
}

type Task struct {
	Id      string
	Meta    TaskMeta
	Command TaskCommand

	Status string
}
