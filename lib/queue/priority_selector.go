package queue

import (
	"math/rand"
	"sort"
	"time"
)

var random = rand.New(rand.NewSource(time.Now().UnixNano()))

type Priority = uint8

func SelectPriority(priorityOptions []Priority) (Priority, bool) {
	// Select priority from existing priorities
	options := make([]int, 0)
	for _, opt := range priorityOptions {
		options = append(options, int(opt))
	}
	sort.Ints(options)
	sum := 0
	for _, option := range options {
		sum += option
	}
	chosen := random.Float32()
	high, low := float32(1.0), float32(0.0)
	for i := len(options) - 1; i >= 0; i-- {
		low = high - float32(options[i])/float32(9)
		if low <= chosen && high >= chosen {
			return Priority(options[i]), true
		} else {
			high = low
		}
	}
	return Priority(0), false
}
