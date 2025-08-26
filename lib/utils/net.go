package utils

import (
	"fmt"
	"io"
	"net"
	"time"
)

type WithNetConnOption func(conn net.Conn)

func WithReadDeadline(duration time.Duration) WithNetConnOption {
	return func(conn net.Conn) {
		_ = conn.SetReadDeadline(time.Now().Add(duration))
	}
}

func WithWriteDeadline(duration time.Duration) WithNetConnOption {
	return func(conn net.Conn) {
		_ = conn.SetWriteDeadline(time.Now().Add(duration))
	}
}

func WriteAll(conn net.Conn, data []byte, options ...WithNetConnOption) error {
	for _, option := range options {
		option(conn)
	}
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

func ReadAll(conn net.Conn, options ...WithNetConnOption) ([]byte, error) {
	for _, option := range options {
		option(conn)
	}

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
