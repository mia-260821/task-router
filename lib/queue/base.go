package queue

type Message = interface{}

type Queue interface {
	Enqueue(message Message) error
	Dequeue() Message
	
	Empty() bool
	Full() bool
	Close()
}
