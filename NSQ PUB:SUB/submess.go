package main

import (
  "log"
  "fmt"
  "time"
  "os"
  "strconv"
  "github.com/nsqio/go-nsq"
)

//for further implementation
func HandleMsg(message []byte) {
  //only to print out
  s := string(message[:5])
  fmt.Printf("The message is %s \n",s)
}

func test(Channel string) {
  //assign topic&channel, config consumer
  channel := Channel 
  topic := channel
  config := nsq.NewConfig()
  config.MaxInFlight = 2500
  config.OutputBufferSize = -1
  sub, _ := nsq.NewConsumer(topic, channel, config)
  
  //design handler's handle function
  sub.AddHandler(nsq.HandlerFunc(func(message *nsq.Message) error {
      HandleMsg(message.Body)
      return nil
  }))
  
  //connect
  err := sub.ConnectToNSQLookupd("18.221.119.174:4161")
  if err != nil {
  log.Panic("Could not connect")
  }
}

func main() {
  //usage go run sub.go <number of topic to subscribe> <topic raw name>
  if len(os.Args) < 3 {
    fmt.Println("Please input souscriber_number&ctopic(channel) as well")
  }
  
  sub_num, _ := strconv.Atoi(os.Args[1])
  topic := os.Args[2]
  
  //subscribe to each topic
  for i := 0 ; i< sub_num ; i++ {
    fmt.Printf("Right now is sublisher %d \n",i)
    sub_str := strconv.Itoa(i)
    fmt.Printf("The topic is %+v \n",topic+sub_str)
    go test(topic) 
  }
  
  for {
    time.Sleep(20 * time.Second)
  }
}
