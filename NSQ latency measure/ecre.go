package main

import (
	"os"
	"runtime"
	"strconv"
	"time"

	"log"
	"io/ioutil"
	"github.com/nsqio/go-nsq"
	//"bytes"
	"encoding/binary"
	//"fmt"
)
type MessageHandler interface {
	ReceiveMessage([]byte) bool
}

type LatencyMessageHandler struct {
	NumberOfMessages int
	Latencies        []float32
	Results          []byte
	messageCounter   int
	Channel          string
}

func (handler *LatencyMessageHandler) ReceiveMessage(message []byte) bool {
	now := time.Now().UnixNano()
	var then int64

	then, _ = binary.Varint(message[0:18])

	if then != 0 {

		handler.Latencies = append(handler.Latencies, (float32(now-then))/1000/1000)

		x := strconv.FormatInt((now-then)/1000/1000, 10)
		handler.Results = append(handler.Results, x...)
		handler.Results = append(handler.Results, "\n"...)
		//}
	}
	k:= handler.NumberOfMessages *1
	//log.Printf("%d",k)
	handler.messageCounter++
	if handler.messageCounter == k*7 {
		//log.Printf("13000")
		sum := float32(0)
		for _, latency := range handler.Latencies {
			sum += latency

		}
		avgLatency := float32(sum) / float32(len(handler.Latencies))
		log.Printf("Mean latency for %d messages: %f ms\n", handler.messageCounter, avgLatency)
		ioutil.WriteFile("latency", handler.Results, 0777)
		//}

	}

	return false
}

func NewNsq(numberOfMessages int, channeL string) {
	channel := channeL
	channel += "n#ephemeral"
	topic := channel
	config := nsq.NewConfig()
	config.MaxInFlight = 2000
	//config.MsgTimeout = 150000
	config.OutputBufferSize = -1
	sub, _ := nsq.NewConsumer(topic, channel, config)
	var handler MessageHandler
	handler = &LatencyMessageHandler{
		NumberOfMessages: numberOfMessages,
		Latencies:        []float32{},
		Channel:          channeL,
	}

	sub.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
		handler.ReceiveMessage(message.Body)
		return nil
	}))

	//sub.ConnectToNSQD("192.168.1.11:4150")
	
	err := sub.ConnectToNSQLookupd("18.221.119.174:4161")
	if err != nil {
		log.Panic("Could not connect")
	}
}

func newTest(msgCount int, channel string) {
	NewNsq(msgCount, channel)
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	num, _ := strconv.Atoi(os.Args[1])
	topic, _ := strconv.Atoi(os.Args[2])
	msg,_ := strconv.Atoi(os.Args[3])
	msg=msg*1000
	for i := 0; i < num; i++ {
		go newTest(msg, strconv.Itoa(topic+i)) //parseArgs(usage)
	}
	for {
		time.Sleep(20 * time.Second)
	}
}