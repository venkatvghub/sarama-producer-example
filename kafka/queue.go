package kafka

import (
	"context"
	"fmt"
)

// Queue ... A generic Queue Interface. This interface is largely defined for handling fallback queues besides kafka
// Note: This library by itself will not handle fallbacks. You need to handle that by yourself. This interface,
// allows you to extend the same for your purposes
type Queue interface {
	// Specifically for kafka, this is async and might not produce any error(or not noticeable). We are still returning
	// a generic error type for sanity with other providers in case of sync based production
	SendMessage(ctx context.Context, msg string, qname string) error
	// Generic Disconnect method
	Disconnect()
	//Retry
	Retry(ctx context.Context, msg string, qname string) error
}

// NewQueue ... Queue Initialization
func NewProducerQueue(ctx context.Context, driver string, config interface{}) (Queue, error) {
	var queue Queue
	var err error
	switch driver {
	case "kafka":
		pConfig := config.(*ProducerConfig)
		queue, err = NewKafkaProducer(ctx, pConfig)
	default:
		return nil, fmt.Errorf("Queue Driver %s not supported", driver)
	}
	return queue, err
}
