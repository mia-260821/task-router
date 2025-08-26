package lib

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"task-router/lib/utils"
	"time"
)

type ServiceInfo struct {
	Name string
	Host string
	Port uint16
}

type ServiceRecord struct {
	info   ServiceInfo
	status utils.HeartbeatResult

	heartbeatCancel context.CancelFunc
}

type NameService interface {
	Lookup(string) []ServiceInfo
	Register(info ServiceInfo) error
	Deregister(info ServiceInfo)
}

type NameServiceImpl struct {
	listenIp   string
	listenPort uint16

	mutex    sync.RWMutex
	registry []*ServiceRecord

	ctx context.Context
}

func NewNameService(ctx context.Context, ip string, port uint16) NameService {
	return &NameServiceImpl{registry: make([]*ServiceRecord, 0), ctx: ctx, listenIp: ip, listenPort: port}
}

func (ns *NameServiceImpl) Lookup(name string) []ServiceInfo {
	ns.mutex.RLock()
	defer ns.mutex.RUnlock()

	result := make([]ServiceInfo, 0)
	for _, record := range ns.registry {
		if strings.EqualFold(record.info.Name, name) && record.status.Status == utils.HeartbeatStatusUp {
			result = append(result, record.info)
		}
	}
	return result
}

func (ns *NameServiceImpl) Register(info ServiceInfo) error {
	ns.mutex.Lock()
	defer ns.mutex.Unlock()

	for _, record := range ns.registry {
		if record.info == info {
			return fmt.Errorf("service %s already exists", info.Name)
		}
	}
	newRecord := &ServiceRecord{info: info}
	newRecord.status.Status = utils.HeartbeatStatusDown

	var hbCtx context.Context
	hbCtx, newRecord.heartbeatCancel = context.WithCancel(ns.ctx)
	go ns.StartHeartbeat(hbCtx, newRecord)

	ns.registry = append(ns.registry, newRecord)
	return nil
}

func (ns *NameServiceImpl) Deregister(info ServiceInfo) {
	ns.mutex.Lock()
	defer ns.mutex.Unlock()

	for i, record := range ns.registry {
		if record.info == info {
			record.heartbeatCancel()
			ns.registry = append(ns.registry[:i], ns.registry[i+1:]...)
		}
	}
}

func (ns *NameServiceImpl) StartHeartbeat(ctx context.Context, record *ServiceRecord) {
	heartbeat := utils.NewHeartbeat(5*time.Second, 30*time.Second, record.info.Host, record.info.Port)
	out := heartbeat.Start(ctx)
	for {
		select {
		case <-ctx.Done():
			record.heartbeatCancel()
			return
		case result := <-out:
			record.status = result
			return
		}
	}
}

func (ns *NameServiceImpl) Start() error {
	addr := net.UDPAddr{
		IP:   net.ParseIP(ns.listenIp),
		Port: int(ns.listenPort),
	}
	listener, err := net.ListenUDP("udp", &addr)
	if err != nil {
		return fmt.Errorf("failed to bind to UDP %s:%d: %v", ns.listenIp, ns.listenPort, err)
	}
	defer listener.Close()

	buf := make([]byte, 1024)
	for {
		n, remoteAddr, err := listener.ReadFromUDP(buf)
		if err != nil {
			fmt.Println("Read error:", err)
			continue
		}
		data := string(buf[:n])
		fmt.Printf("Received %d bytes from %s: %s\n", n, remoteAddr, data)

		result := ns.Lookup(data)

		block, ok := utils.Serialize(result)
		if !ok {
			return fmt.Errorf("failed to serialize")
		}
		if _, err = listener.WriteToUDP(block, remoteAddr); err != nil {
			fmt.Println("Write error:", err)
			continue
		}
		fmt.Printf("Sent response to %s\n", remoteAddr)
	}
}
