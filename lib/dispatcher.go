package lib

import (
	"log"
	"task-router/lib/utils"
)

var (
	logger = log.Default()
)

type Worker struct{}

func (worker *Worker) Run(task Task) {
}

type WorkerManger struct {
	workers []any
}

func NewWorkerManger() *WorkerManger {
	return &WorkerManger{}
}

func (m *WorkerManger) AddWorker(worker any) {
	m.workers = append(m.workers, worker)
}

func (m *WorkerManger) FindWorker(requirement TaskMeta) (*Worker, bool) {
	return &Worker{}, true
}

type Dispatcher struct {
	workerMgr    WorkerManger
	brokerClient BrokerClient

	retryList []*Task
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		retryList: make([]*Task, 100),
	}
}

func (d *Dispatcher) Start() {
	// fetch a task
	// assign to worker
	for {
		out, err := d.brokerClient.Consume(20)
		if err != nil {
			panic(err)
		}
		for v := range out {
			d.process(v)
		}
	}
}

func (d *Dispatcher) process(block []byte) {
	// collect task info
	// find a proper worker
	// assign the task to worker
	// monitor the task execution
	task, ok := utils.Deserialize[Task](block)
	if !ok {
		logger.Println("Deserialize task fail")
		return
	}
	worker, ok := d.workerMgr.FindWorker(task.Meta)
	if !ok {
		task.Status = "PENDING_FOR_WORKER"
		d.retryList = append(d.retryList, &task)
		return
	}
	worker.Run(task)
}

func (d *Dispatcher) Stop() {}
