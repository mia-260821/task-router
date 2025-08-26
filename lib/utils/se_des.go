package utils

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

func Serialize[T any](data T) ([]byte, bool) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(data); err != nil {
		fmt.Println(err)
		return nil, false
	}
	return buf.Bytes(), true
}

func Deserialize[T any](block []byte) (T, bool) {
	var data T
	buf := bytes.NewBuffer(block)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&data); err != nil {
		fmt.Println(err)
		return data, false
	}
	return data, true
}
