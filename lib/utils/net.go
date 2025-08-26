package utils

import (
	"fmt"
	"io"
	"net"
)

func WriteAll(conn net.Conn, data []byte) error {
	total := 0
	for total < len(data) {
		n, err := conn.Write(data[total:])
		if err != nil {
			return err
		}
		total += n
	}
	return nil
}

func ReadAll(conn net.Conn) ([]byte, error) {
	data := make([]byte, 1024)
	count := 0

	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("error reading from connection" + err.Error())
		}
		data = append(data, buf[:n]...)
		count += n
	}
	return data[:count], nil
}
