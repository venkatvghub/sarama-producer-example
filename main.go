package main

import (
	"github.com/venkatvghub/sarama-producer-example/kafka"
	"os"
	"os/signal"
	"time"
	"fmt"
)

var signals = make(chan os.Signal, 1)

func produceAsyncMessages(producer *kafka.KafkaQueue){
	i := 0
	for {
		time.Sleep(500 * time.Millisecond)
		//go ticker_clock(ticker)
		msgStr := fmt.Sprintf("Hello Go! :%d", i)
		producer.SendMessage(msgStr, "test")
		go handleStatus(producer)
		i +=1
	}
	defer producer.Disconnect()
}

func handleStatus(producer *kafka.KafkaQueue){
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-producer.Producer.Errors():
				msg, _ := err.Msg.Value.Encode()
				fmt.Printf("Failed to produce Message [%v] to Partition [%v], Offset:[%v]\n", string(msg), err.Msg.Partition, err.Msg.Offset)
			case success := <-producer.Producer.Successes():
				fmt.Printf("Successfully produced Message [%s] to Partition [%v], Offset:[%v]\n", success.Value, success.Partition, success.Offset)
			case <-signals:
				doneCh <- struct{}{}
				producer.Disconnect()
			}
		}
		time.Sleep(500 * time.Millisecond)
	}()
	<-doneCh
}

func main(){
	signal.Notify(signals, os.Interrupt)
	cfg := kafka.ProducerConfig{
		RetryBackoff:      6 * time.Second,
		Partitioner:       "random",
		MaxRetry:          3,
		MaxMessages:       100,
		CompresionEnabled: true,
		CompressionType:   "snappy",
		Brokers:           []string{"127.0.0.1:9092"},
		EnableTLS:         false,
	}
	producer, err := kafka.NewKafkaProducer(&cfg)
	if err != nil {
		fmt.Printf("Error creating kafka producer:%v\n", err)
	}
	//producer.SendMessage("Hello World", "test")
	produceAsyncMessages(producer)
}