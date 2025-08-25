package queue

import (
	"errors"
	"fmt"
)

type MessageQueue struct {
	queue  chan interface{}
	closed bool
}

func NewMessageQueue(capacity uint32) Queue {
	return &MessageQueue{
		queue:  make(chan interface{}, capacity),
		closed: false,
	}
}

func (t *MessageQueue) Enqueue(message Message) error {
	// might block
	if t.Full() {
		return errors.New("task queue is full")
	}
	if t.closed {
		return errors.New("task queue is closed")
	}
	t.queue <- message
	return nil
}

func (t *MessageQueue) Dequeue() Message {
	if t.Empty() {
		return nil
	}
	val, ok := <-t.queue
	if !ok { // Channel is closed
		return nil
	}
	return val
}

func (t *MessageQueue) Close() {
	if t.closed {
		return
	}
	defer close(t.queue)
	t.closed = true
}

func (t *MessageQueue) Empty() bool {
	return len(t.queue) == 0
}

func (t *MessageQueue) Full() bool {
	return len(t.queue) >= cap(t.queue)
}

type PriorityMessage interface {
	Priority() Priority
}

type PriorityMessageQueue struct {
	queues     map[Priority]Queue
	priorities []Priority
	capacity   uint32
}

func NewPriorityMessageQueue(priorityOptions []Priority, capacity uint32) Queue {
	queue := &PriorityMessageQueue{
		queues:     make(map[Priority]Queue),
		priorities: priorityOptions,
		capacity:   capacity,
	}
	return queue
}

func (t *PriorityMessageQueue) getQueue(priority Priority) Queue {
	for _, p := range t.priorities {
		if p == priority {
			if mq, ok := t.queues[p]; ok {
				return mq
			}
			t.queues[p] = NewMessageQueue(t.capacity)
			return t.queues[p]
		}
	}
	return nil
}

func (t *PriorityMessageQueue) Enqueue(message Message) error {
	if msg, ok := message.(PriorityMessage); !ok {
		return errors.New("message is not a PriorityMessage")
	} else {
		return t.enqueue(msg)
	}
}

func (t *PriorityMessageQueue) enqueue(message PriorityMessage) error {
	queue := t.getQueue(message.Priority())
	if queue == nil {
		return errors.New("wrong priority")
	}
	if err := queue.Enqueue(message); err != nil {
		return fmt.Errorf("add message to queue (priority=%v) failed", message.Priority())
	}
	return nil
}

func (t *PriorityMessageQueue) Dequeue() Message {
	var priority Priority
	options := append([]Priority(nil), t.priorities...)
	for len(options) > 0 {
		var ok bool
		if priority, ok = SelectPriority(options); !ok {
			return nil
		}
		queue, ok := t.queues[priority]
		if !ok || queue.Empty() {
			fmt.Printf("queue %v is empty\n", priority)
			for i, v := range options {
				if v == priority {
					options = append(options[:i], options[i+1:]...)
				}
			}
			continue
		} else {
			return queue.Dequeue()
		}
	}
	return nil
}

func (t *PriorityMessageQueue) Close() {
	for priority, queue := range t.queues {
		fmt.Println("Closing queue", priority)
		queue.Close()
	}
}

func (t *PriorityMessageQueue) Empty() bool {
	for _, q := range t.queues {
		if !q.Empty() {
			return false
		}
	}
	return true
}

func (t *PriorityMessageQueue) Full() bool {
	for _, q := range t.queues {
		if !q.Full() {
			return false
		}
	}
	return true
}
