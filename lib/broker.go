package lib

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"task-router/lib/queue"
	"task-router/lib/utils"
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
	buf, err := utils.ReadAll(conn)
	if err != nil {
		fmt.Println("Error reading role:", err)
		return
	}

	greeting := string(buf)
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
	if err := utils.WriteAll(conn, []byte("HELLO FROM SERVER")); err != nil {
		fmt.Println("error writing to connection" + err.Error())
		return
	}
	// Read MESSAGE
	buffer, err := utils.ReadAll(conn)
	if err != nil {
		fmt.Println("error reading from connection" + err.Error())
		return
	} else if len(buffer) == 0 {
		fmt.Println("empty message")
		return
	}

	reply := "ID:FAKE"
	// Enqueue MESSAGE
	err = s.queue.Enqueue(buffer)
	if err != nil {
		fmt.Println("error enqueuing message" + err.Error())
		reply = "ERR"
	}
	// Send REPLY
	if err = utils.WriteAll(conn, []byte(reply)); err != nil {
		fmt.Println("error writing to connection" + err.Error())
	}
}

func (s *BrokerServer) handleConsumer(conn net.Conn) {
	if err := utils.WriteAll(conn, []byte("HELLO FROM SERVER")); err != nil {
		fmt.Println("error writing to connection" + err.Error())
		return
	}
	// Read Number of Messages
	size := 0
	buffer, err := utils.ReadAll(conn)
	if err != nil {
		fmt.Println("error reading from connection" + err.Error())
		return
	}
	size, err = strconv.Atoi(string(buffer))
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
		} else if err = utils.WriteAll(conn, block); err != nil {
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
	if err = utils.WriteAll(conn, data); err != nil {
		return "", fmt.Errorf("could not send message: " + err.Error())
	}
	reply, err := utils.ReadAll(conn)
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
	err := utils.WriteAll(conn, []byte(fmt.Sprintf("HELLO FROM %s", strings.ToUpper(role))))
	if err != nil {
		return fmt.Errorf("error sending to connection" + err.Error())
	}

	// Read Reply to Role
	buffer, err := utils.ReadAll(conn)
	if err != nil {
		return fmt.Errorf("error sending to connection" + err.Error())
	}
	if string(buffer) != "HELLO FROM SERVER" {
		return fmt.Errorf("wrong reply from server: " + string(buffer))
	}
	return nil
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
	if err = utils.WriteAll(conn, []byte("10")); err != nil {
		return nil, fmt.Errorf("could not send message: " + err.Error())
	}
	// Read
	size := 10

	out := make(chan []byte, size)
	defer close(out)

	go func() {
		for i := 0; i < size; i++ {
			buffer, err := utils.ReadAll(conn)
			if err != nil {
				fmt.Println("could not receive message: " + err.Error())
				break
			}
			out <- buffer
		}
	}()
	return out, nil
}
