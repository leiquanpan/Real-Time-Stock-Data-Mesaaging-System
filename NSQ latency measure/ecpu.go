package main

import (
	"encoding/binary"
	"log"
	"os"
	"runtime"
	"strconv"
	"time"
	//"go/format"
	//"github.com/nsqio/mq"
	"github.com/nsqio/go-nsq"
	//"fmt"
	"net"
	"fmt"
)
type Nsq struct {
	pub       *nsq.Producer
	msgCount  int
	msgSize   int
	topic     string
	topic_raw string
}

func NewNsq(msgCount int, msgSize int, topic_raw string) *Nsq {
	topic := topic_raw
	topic += "n#ephemeral"

	//pub, _ := nsq.NewProducer("localhost:4150", nsq.NewConfig())
	pub, _ := nsq.NewProducer("18.221.119.174:4152", nsq.NewConfig())
	//_ = pub.ConnectToNSQLookupd_v2(lookupd, priority)

	return &Nsq{
		pub:       pub,
		msgCount:  msgCount,
		msgSize:   msgSize,
		topic:     topic,
		topic_raw: topic_raw,
	}
}

func (n *Nsq) Teardown() {
	n.pub.Stop()
}

func (n *Nsq) Send(message []byte) {
	message = append(message, n.topic_raw...)
	message = append(message, "\n"...)
	b := make([]byte, n.msgSize-len(message))
	message = append(message, b...)
	n.pub.PublishAsync(n.topic, message, nil)
}

func newTest(msgCount, msgSize int, topic string, gap int) {
	nsq := NewNsq(msgCount, msgSize, topic)
	start := time.Now().UnixNano()
	b := make([]byte, 24)
	id := make([]byte, 5)
	for i := 0; i < msgCount; i++ {
		if i == 1 {
			time.Sleep(5 * time.Second)
		}
		//t1 :=time.Now()
		//fmt.Println(t1,t1.UnixNano())
		//t1 :=time.Now().UnixNano()
		binary.PutVarint(b, time.Now().UnixNano())
		binary.PutVarint(id, int64(i))
		//fmt.Printf("len de time : %d",len(string(t1)))
		//fmt.Printf("len de ID: %d", len(id))

		copy(b[19:23], id[:])

		nsq.Send(b)

		// inter-msg gap
		time.Sleep(time.Duration(gap) * time.Microsecond)
	}

	stop := time.Now().UnixNano()
	ms := float32(stop-start) / 1000000
	log.Printf("Sent %d messages in %f ms\n", msgCount, ms)
	log.Printf("Sent %f per second\n", 1000*float32(msgCount)/ms)

	//	nsq.Teardown()
	for {
		time.Sleep(50 * time.Second)
	}
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	num, _ := strconv.Atoi(os.Args[1])
	topic, _ := strconv.Atoi(os.Args[2])
	gap, _ := strconv.Atoi(os.Args[3])
	msg, _ := strconv.Atoi(os.Args[4])
	msg=msg*1000
	//srcAddr := &net.UDPAddr{IP: net.IPv4zero, Port: 0}

	msg =msg+1
	log.Print(msg)
	for i := 0; i < num; i++ {

		go newTest(msg, 512, strconv.Itoa(topic+i), gap)
	}
	for {
		time.Sleep(50 * time.Second)
	}

}