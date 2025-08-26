package lib

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

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
}

func SerializeTask[T any](data T) ([]byte, bool) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(data); err != nil {
		fmt.Println(err)
		return nil, false
	}
	return buf.Bytes(), true
}

func DeserializeTask[T any](block []byte) (T, bool) {
	var data T
	buf := bytes.NewBuffer(block)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&data); err != nil {
		fmt.Println(err)
		return data, false
	}
	return data, true
}
