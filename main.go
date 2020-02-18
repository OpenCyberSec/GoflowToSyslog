package main

import (
	"log"
	"time"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/GolangResources/maptoleef/v1"
	"github.com/GolangResources/syslog/syslog"
	flow "github.com/cloudflare/flow-pipeline/pb-ext"
	proto "github.com/golang/protobuf/proto"
	"os"
	"os/signal"
	"syscall"
	"sync"
	"fmt"
	"net"

)

const (
	CHAN_NUMBER = 10
)

var msgc chan string
var mapt maptoleef.LEEF

var conn *syslog.Writer

type SKafka struct {
	Broker	string
	Group	string
	Topic	string
}

func sendworker() {
        for msg := range msgc {
		time.Sleep(1)
		var fmsg flow.FlowMessage
		err := proto.Unmarshal([]byte(msg), &fmsg)
		if err != nil {
			log.Println("ERROR", err)
		}
		ts := time.Unix(int64(fmsg.TimeFlow), 0)
		srcip := net.IP(fmsg.SrcIP)
		dstip := net.IP(fmsg.DstIP)
		srcipstr := srcip.String()
		dstipstr := dstip.String()
		if srcipstr == "<nil>" {
			srcipstr = "0.0.0.0"
		}
		if dstipstr == "<nil>" {
			dstipstr = "0.0.0.0"
		}
		extract := map[string]interface{} {
			"ts": ts,
			"Type": fmsg.Type,
			"SamplingRate": fmsg.SamplingRate,
			"SrcIP": srcipstr,
			"DstIP": dstipstr,
			"Bytes": fmsg.Bytes,
			"Packets": fmsg.Packets,
			"SrcPort": fmsg.SrcPort,
			"DstPort": fmsg.DstPort,
			"Etype": fmsg.Etype,
			"Proto": fmsg.Proto,
			"SrcAS": fmsg.SrcAS,
			"DstAS": fmsg.DstAS,

		}
		msg := mapt.MapToLEEF(extract)
		conn.Notice(&msg, mapt.ProductName)
	}
}

func kafkareader(waitgroup *sync.WaitGroup, s SKafka) {
        defer waitgroup.Done()
	msgc = make(chan string)
	for j := 0; j < CHAN_NUMBER; j++ {
		go sendworker()
        }
        var topics []string
        topics = append(topics, s.Topic)
        sigchan := make(chan os.Signal, 1)
        signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
        c, err := kafka.NewConsumer(&kafka.ConfigMap{
                "bootstrap.servers":                    s.Broker,
                "group.id":                             s.Group,
                "session.timeout.ms":                   6000,
                "go.events.channel.enable":             true,
                "go.application.rebalance.enable":      true,
                "default.topic.config":                 kafka.ConfigMap{"auto.offset.reset": "largest"}})
        if err != nil {
                fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
                os.Exit(1)
        }
        fmt.Printf("Created Consumer %v\n", c)
        err = c.SubscribeTopics(topics, nil)
        run := true
        for run == true {
                select {
                case sig := <-sigchan:
                        fmt.Printf("Caught signal %v: terminating\n", sig)
                        run = false
                case ev := <-c.Events():
                        switch e := ev.(type) {
                        case kafka.AssignedPartitions:
                                fmt.Fprintf(os.Stderr, "%% %v\n", e)
                                c.Assign(e.Partitions)
                        case kafka.RevokedPartitions:
                                fmt.Fprintf(os.Stderr, "%% %v\n", e)
                                c.Unassign()
                        case *kafka.Message:
                                msgc <- string(e.Value)
                        case kafka.PartitionEOF:
                                fmt.Printf("%% Reached %v\n", e)
                        case kafka.Error:
                                fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
                                run = false
                        }
                }
        }
        fmt.Printf("Closing consumer\n")
        c.Close()
}

func main() {
	mapt = maptoleef.LEEF{
                Manufacter: "GoFlow",
                ProductName: "GoFlow",
                ProductVersion: "1",
                EventID: "1",
                Delimiter: "",
        }
	skafka := SKafka{
		Broker: "broker:port",
		Group: "group",
		Topic: "flows",
	}
	var err error
	conn, err = syslog.Dial("ctcp", "syslogserver:port", syslog.LOG_NOTICE, nil)
	defer conn.Close()
	if (err != nil) {
		panic(err)
	}
	var waitgroup sync.WaitGroup
	log.Println("Launch kafka")
	waitgroup.Add(1)
	go kafkareader(&waitgroup, skafka)
	waitgroup.Wait()
	log.Println("All done kafka")
}
