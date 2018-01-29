package main

import (
  "log"
  "fmt"
  "os"
  "strconv"
  "time"
  "encoding/json" 
  "github.com/nsqio/go-nsq"
)
type Response1 struct {
    ticker string 
    date string
    open float64
    high float64 
    low float64
    close float64
    volume float64
    ex_dividend float64
    split_ratio float64
    adj_open float64
    adj_high float64
    adj_low float64
    adj_close float64
    adj_volume float64
}

func test(msg_count int,topic string){
  //create publisher for the topic
  config := nsq.NewConfig()
  w, _ := nsq.NewProducer("18.221.119.174:4152", config)
 fmt.Println(topic) 
  //publish n messages 
  for i:=0 ; i<msg_count ; i++ {
    i_str := strconv.Itoa(i)
    fmt.Println("message is publishing, the content is \n", "test"+i_str)
    err := w.Publish(topic, []byte("test"+i_str))
    if err != nil {
      log.Panic("Could not connect")
    }
    time.Sleep(1024 * time.Microsecond)
  }
  w.Stop()
}
func runmess(mess string, topic string){
  config :=nsq.NewConfig()
  w,_ := nsq.NewProducer("18.221.119.174:4152", config)
  fmt.Println("in run mess \n",topic)
  fmt.Println("message is publishing, the content is \n", mess)
  err := w.Publish(topic, []byte(mess))
    if err != nil {
      log.Panic("Could not connect")
    }
    time.Sleep(1024 * time.Microsecond)
    w.Stop()
}
func main() {
  //usage go run pub.go <numof publisher> <numof messages for each publisher> <topic raw name>
  if len(os.Args) < 3 {
  fmt.Println("Please input topic &message as well")
  }

  //pub_num, _ := strconv.Atoi(os.Args[1])
  //num, _ := strconv.Atoi(os.Args[2])
  topic := os.Args[1]
  mess:=os.Args[2]
  res :=Response1{}
  json.Unmarshal([]byte(mess), &res)
  fmt.Println(res)

  
  //create many publisher 
  //fmt.Printf("Right now is publisher %d \n",i)
  //pub_str := strconv.Itoa(i)
  //go runmess(topic+pub_str+"m#ephemeral") 
  go runmess(mess,topic)

  for i := 0 ; i < 1024 ; i++ {
    time.Sleep(20 * time.Microsecond) 
  }
}
