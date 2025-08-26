package lib

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
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
	if _, err := conn.Write([]byte("HELLO FROM SERVER")); err != nil {
		fmt.Println("error writing to connection" + err.Error())
		return
	}
	// Read Number of Messages
	size := 0
	buffer := make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		fmt.Println("error reading from connection" + err.Error())
		return
	}
	size, err = strconv.Atoi(string(buffer[:n]))
	if err != nil {
		fmt.Println("error converting size to int" + err.Error())
		return
	}
	for i := 0; i < size; i++ {
		msg := s.queue.Dequeue()
		if msg == nil {
			fmt.Println("error dequeue message")
			break
		}
		if block, ok := msg.([]byte); !ok {
			fmt.Println("error convert message to bytes	")
			break
		} else if _, err := conn.Write(block); err != nil {
			fmt.Println("error writing to connection" + err.Error())
			break
		}
	}
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
		if err := ctx.Err(); err != nil {
			fmt.Println("context cancelled" + err.Error())
			return err
		}
		return nil
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

	err = c.confirmRole(conn, "producer")
	if err != nil {
		return "", fmt.Errorf("could not confirm role for consumer: " + err.Error())
	}

	// Send Message
	if err = c.send(conn, data); err != nil {
		return "", fmt.Errorf("could not send message: " + err.Error())
	}
	reply, err := c.receive(conn)
	if err != nil {
		return "", fmt.Errorf("could not receive message: " + err.Error())
	}
	fmt.Println("Received reply to produce message:" + string(reply))

	segments := strings.Split(string(reply), ":")
	if segments[0] == "ID" {
		return segments[1], nil
	}
	return "", errors.New("invalid message reply")
}

func (c *BrokerClient) confirmRole(conn net.Conn, role string) error {
	// Send Role
	err := c.send(conn, []byte(fmt.Sprintf("HELLO FROM %s", strings.ToUpper(role))))
	if err != nil {
		return fmt.Errorf("error sending to connection" + err.Error())
	}

	// Read Reply to Role
	buffer, err := c.receive(conn)
	if err != nil {
		return fmt.Errorf("error sending to connection" + err.Error())
	}
	if string(buffer) != "HELLO FROM SERVER" {
		return fmt.Errorf("wrong reply from server: " + string(buffer))
	}
	return nil
}

func (c *BrokerClient) send(conn net.Conn, data []byte) error {
	n, err := conn.Write(data)
	if err != nil {
		return fmt.Errorf("error writing to connection" + err.Error())
	} else if n != len(data) {
		return fmt.Errorf("error writing to connection")
	}
	return nil
}

func (c *BrokerClient) receive(conn net.Conn) ([]byte, error) {
	var buffer = make([]byte, 1024)
	n, err := conn.Read(buffer)
	if err != nil {
		return nil, fmt.Errorf("read hello from server failed: %v", err)
	} else {
		return buffer[:n], nil
	}
}

func (c *BrokerClient) Consume(n uint16) (<-chan []byte, error) {
	remoteAddr := fmt.Sprintf("%s:%d", c.serverHost, c.serverPort)
	conn, err := net.Dial("tcp", remoteAddr)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	err = c.confirmRole(conn, "consumer")
	if err != nil {
		return nil, fmt.Errorf("could not confirm role for consumer: " + err.Error())
	}

	// Send batch size
	if err = c.send(conn, []byte("10")); err != nil {
		return nil, fmt.Errorf("could not send message: " + err.Error())
	}
	// Read
	size := 10

	out := make(chan []byte, size)
	defer close(out)

	go func() {
		for i := 0; i < size; i++ {
			buffer, err := c.receive(conn)
			if err != nil {
				fmt.Println("could not receive message: " + err.Error())
				break
			}
			out <- buffer
		}
	}()
	return out, nil
}
