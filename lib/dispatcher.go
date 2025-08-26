package lib

type WorkerManger struct {
	workers []any
}

func NewWorkerManger() *WorkerManger {
	return &WorkerManger{}
}

func (m *WorkerManger) AddWorker(worker any) {
	m.workers = append(m.workers, worker)
}

type Dispatcher struct {
	workerMgr    WorkerManger
	brokerClient BrokerClient

	retryList []interface{}
}

func NewDispatcher() *Dispatcher {
	return &Dispatcher{
		retryList: make([]interface{}, 100),
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

func (d *Dispatcher) process(v interface{}) {
	// collect task info
	// find a proper worker
	// assign the task to worker
	// monitor the task execution
}

func (d *Dispatcher) Stop() {}
