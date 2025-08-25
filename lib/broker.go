package lib

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"task-router/lib/queue"
)

type BrokerServer struct {
	port  uint16
	queue queue.Queue
}

func NewBrokerServer(port uint16) *BrokerServer {
	return &BrokerServer{port: port}
}

func (s *BrokerServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	// Read Client Role: Producer or Consumer
	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		fmt.Println("error reading from connection" + err.Error())
		return
	}

	greeting := string(buffer[:n])
	switch greeting {
	case "HELLO FROM PRODUCER":
		s.handleProducer(conn)
		break
	case "HELLO FROM CONSUMER":
		s.handleConsumer(conn)
		break
	default:
		fmt.Println("unknown greeting", greeting)
	}
}

func (s *BrokerServer) handleProducer(conn net.Conn) {
	if _, err := conn.Write([]byte("HELLO FROM SERVER")); err != nil {
		fmt.Println("error writing to connection" + err.Error())
		return
	}
	// Read MESSAGE
	buffer := make([]byte, 10240)
	n, err := conn.Read(buffer)
	if err != nil {
		fmt.Println("error reading from connection" + err.Error())
		return
	}

	reply := "ID:FAKE"
	// Enqueue MESSAGE
	data := buffer[:n]
	err = s.queue.Enqueue(data)
	if err != nil {
		fmt.Println("error enqueuing message" + err.Error())
		reply = "ERR"
	}
	// Send REPLY
	_, err = conn.Write([]byte(reply))
	if err != nil {
		fmt.Println("error writing to connection" + err.Error())
	}
}

func (s *BrokerServer) handleConsumer(conn net.Conn) {
	panic("unimplemented")
}

func (s *BrokerServer) Start(ctx context.Context) error {
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4zero, Port: int(s.port)})
	if err != nil {
		return errors.New("Could not start server: " + err.Error())
	}
	defer listener.Close()
	defer fmt.Println("Server stopped")

	stopped := make(chan interface{})
	defer close(stopped)

	go func() {
		for {
			conn, err := listener.AcceptTCP()
			if err != nil {
				fmt.Println("Could not accept connection: " + err.Error())
				break
			}
			go s.handleConnection(conn)
		}
		stopped <- true
	}()

	select {
	case <-stopped:
		return errors.New("server stopped")
	case <-ctx.Done():
		return errors.New("server canceled")
	}
}

type BrokerClient struct {
	serverHost string
	serverPort uint16
}

func NewBrokerClient(serverHost string, serverPort uint16) *BrokerClient {
	return &BrokerClient{serverHost: serverHost, serverPort: serverPort}
}

func (c *BrokerClient) Produce(data []byte) (string, error) {
	remoteAddr := fmt.Sprintf("%s:%d", c.serverHost, c.serverPort)
	conn, err := net.Dial("tcp", remoteAddr)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	// Send Role
	if _, err = conn.Write([]byte("HELLO FROM PRODUCER")); err != nil {
		return "", fmt.Errorf("error writing to connection" + err.Error())
	}

	// Read Reply to Role
	var buffer = make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		return "", fmt.Errorf("read hello from server failed: %v", err)
	} else if string(buffer[:n]) != "HELLO FROM SERVER" {
		return "", fmt.Errorf("wrong hello from server")
	}

	// Send Message
	_, err = conn.Write(data)
	if err != nil {
		return "", fmt.Errorf("error writing to connection" + err.Error())
	}

	// Read reply to Message
	n, err = conn.Read(buffer)
	if err != nil {
		return "", fmt.Errorf("read message reply failed: %v", err)
	} else if n <= 0 {
		return "", errors.New("empty message reply")
	}

	reply := string(buffer[:n])
	segments := strings.Split(reply, ":")
	if segments[0] == "ID" {
		return segments[1], nil
	}
	fmt.Println("Received reply to produce message:" + reply)
	return "", errors.New("invalid message reply")
}

func (c *BrokerClient) Consume(msg string) (string, error) {
	panic("unimplemented")
}
