package lib

import (
	"fmt"
)

type BrokerConfig struct {
	Host string
	Port uint16
}

type Producer struct {
	brokerConfig *BrokerConfig
	brokerClient *BrokerClient
}

func NewProducer(brokerConfig *BrokerConfig) *Producer {
	return &Producer{
		brokerConfig: brokerConfig,
		brokerClient: NewBrokerClient(brokerConfig.Host, brokerConfig.Port),
	}
}

func (p *Producer) Produce(msg []byte) (string, bool) {
	reply, err := p.brokerClient.Produce(msg)
	if err != nil {
		fmt.Println("error producing message: %w", err)
		return "", false
	}
	fmt.Println("produced message reply: ", reply)
	return reply, true
}
