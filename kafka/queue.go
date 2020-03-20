package kafka

import "fmt"

// Queue ... A generic Queue Interface. This interface is largely defined for handling fallback queues besides kafka

type Queue interface {
	// Specifically for kafka, this is async and might not produce any error(or not noticeable). We are still returning
	// a generic error type for sanity with other providers in case of sync based production
	SendMessage(msg string, qname string) error
	// Generic Disconnect method
	Disconnect()
}

// NewQueue ... Queue Initialization
func NewProducerQueue(driver string, config interface{}) (Queue, error) {
	var queue Queue
	var err error
	switch driver {
	case "kafka":
		pConfig := config.(*ProducerConfig)
		queue, err = NewKafkaProducer(pConfig)
	default:
		return nil, fmt.Errorf("Queue Driver %s not supported", driver)
	}
	return queue, err
}
