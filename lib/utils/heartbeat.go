package utils

import (
	"context"
	"fmt"
	"net"
	"time"
)

type HeartbeatStatus = string

const (
	HeartbeatStatusUp   HeartbeatStatus = "UP"
	HeartbeatStatusDown HeartbeatStatus = "DOWN"
)

type HeartbeatResult struct {
	Status HeartbeatStatus
	Time   time.Time
}

type Heartbeat struct {
	lastSuccess time.Time
	interval    time.Duration

	timeout time.Duration

	ip   string
	port uint16
}

func NewHeartbeat(interval, timeout time.Duration, ip string, port uint16) *Heartbeat {
	return &Heartbeat{
		interval: interval,
		timeout:  timeout,
		ip:       ip,
		port:     port,
	}
}

func (h *Heartbeat) Start(ctx context.Context) <-chan HeartbeatResult {
	out := make(chan HeartbeatResult)
	defer close(out)

	go func() {
		ticker := time.NewTicker(h.interval)
		defer ticker.Stop()

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if DetectTcpOpen(h.ip, h.port) {
				h.lastSuccess = time.Now()
				out <- HeartbeatResult{
					Status: HeartbeatStatusUp,
					Time:   time.Now(),
				}
			} else if time.Now().Sub(h.lastSuccess) > h.timeout {
				out <- HeartbeatResult{
					Status: HeartbeatStatusDown,
					Time:   time.Now(),
				}
			}
		}
	}()
	return out
}

func DetectTcpOpen(host string, port uint16) bool {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", host, port), 5*time.Second)
	if err != nil {
		fmt.Printf("Failed to connect to %s:%d: %s\n", host, port, err)
		return false
	}
	_ = conn.Close()
	return true
}
