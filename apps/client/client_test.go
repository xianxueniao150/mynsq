package main

import (
	"github.com/nsqio/go-nsq"
	"log"
	"testing"
	"time"
)

func TestNSQ1(t *testing.T) {
	NSQDsAddrs := []string{"127.0.0.1:4150"}
	go consumer1(NSQDsAddrs)
	go produce1()
	time.Sleep(30 * time.Second)
}

func produce1() {
	cfg := nsq.NewConfig()
	nsqdAddr := "127.0.0.1:4150"
	producer, err := nsq.NewProducer(nsqdAddr, cfg)
	if err != nil {
		log.Fatal(err)
	}
	if err := producer.Publish("test", []byte("x")); err != nil {
		log.Fatal("publish error: " + err.Error())
	}
	if err := producer.Publish("test", []byte("y")); err != nil {
		log.Fatal("publish error: " + err.Error())
	}
}


func consumer1(NSQDsAddrs []string) {
	cfg := nsq.NewConfig()
	consumer, err := nsq.NewConsumer("test", "sensor01", cfg)
	if err != nil {
		log.Fatal(err)
	}
	consumer.AddHandler(nsq.HandlerFunc(
		func(message *nsq.Message) error {
			log.Println(string(message.Body) + " C1")
			return nil
		}))
	if err := consumer.ConnectToNSQDs(NSQDsAddrs); err != nil {
		log.Fatal(err, " C1")
	}
	<-consumer.StopChan
}
