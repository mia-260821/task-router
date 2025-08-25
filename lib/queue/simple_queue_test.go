package queue

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMessageQueue(t *testing.T) {
	q := NewMessageQueue(5)
	defer q.Close()

	assert.True(t, q.Empty(), "New Queue should be empty")
	assert.Nil(t, q.Dequeue(), "New Queue next() should be nil")

	for i := 0; i < 5; i++ {
		msg := fmt.Sprintf("msg-%v", i)
		assert.NoError(t, q.Enqueue(msg), fmt.Sprintf("Enqueue msg %v", msg))
	}
	assert.Error(t, q.Enqueue("oops"), "Adding to a full queue should return an error")

	for i := 0; i < 5; i++ {
		assert.NotNil(t, q.Dequeue(), fmt.Sprintf("%vth value should not be empty", i))
	}
	assert.True(t, q.Empty(), "Queue should be empty")
	assert.NoError(t, q.Enqueue("ok"), "Adding to an empty queue shouldn't return an error")
}

type MyPriorityMsg struct {
	priority Priority
	Msg      Message
}

func (mp MyPriorityMsg) Priority() Priority {
	return mp.priority
}

func TestPriorityMessageQueue(t *testing.T) {
	priorityOptions := make([]Priority, 5)
	for i := 0; i < 5; i++ {
		priorityOptions[i] = Priority(i + 1)
	}
	q := NewPriorityMessageQueue(priorityOptions, 3)
	defer q.Close()

	assert.True(t, q.Empty(), "New Queue should be empty")
	assert.Nil(t, q.Dequeue(), "New Queue next() should be nil")

	assert.Error(t, q.Enqueue("abc"), "Message should be of PriorityMessage type")

	msg := &MyPriorityMsg{Msg: "msg-order-1", priority: priorityOptions[0]}
	assert.NoError(t, q.Enqueue(msg))

	msg = &MyPriorityMsg{Msg: "msg-order-3", priority: priorityOptions[2]}
	assert.NoError(t, q.Enqueue(msg))

	msg = &MyPriorityMsg{Msg: "msg-order-5", priority: priorityOptions[4]}
	assert.NoError(t, q.Enqueue(msg))

	for i := 0; i < 3; i++ {
		msg := q.Dequeue()
		assert.NotNil(t, msg, "Queue should not be empty")
		fmt.Printf("Fetch message %v \n", msg)
		if msg == nil {
			t.FailNow()
		}
	}
	assert.True(t, q.Empty(), "Queues should be empty")
}
