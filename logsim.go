package main

import (
  "fmt"
  "os"
  "time"
  "strings"
  "strconv"
  "sync"
  "io/ioutil"
  "encoding/json"
  "gopkg.in/yaml.v2"
  "github.com/Shopify/sarama"
  "github.com/rcrowley/go-metrics"
)

type KafkaProducer struct {
  BrokerList []string
  Producer sarama.AsyncProducer
  Config sarama.Config
}

type TestConfig struct {
  Count int
  Period int
  Dayoffsets string
  Topic string
  Component string
  Message string
}

type TestConfigs struct {
    TestCfgs []TestConfig `tests`
}


func newTestConfigs(filename string) (*TestConfigs, error) {
  t := &TestConfigs{}
  source, err := ioutil.ReadFile(filename)
  if err != nil {
    panic(err)
  }
  err = yaml.Unmarshal(source, t)
  if err != nil {
    fmt.Printf("error: %v", err)
    panic(err)
  }
  return t, err
}

func newKafkaProducer(brokers []string) (*KafkaProducer, error) {
  var err error
  k := &KafkaProducer{}
  k.BrokerList = brokers

  k.Config = *sarama.NewConfig()
  k.Config.Producer.Return.Successes = true
  k.Config.Producer.Partitioner = sarama.NewHashPartitioner

  k.Producer, err = sarama.NewAsyncProducer(k.BrokerList, &k.Config)
  return k, err
}

func runTests(brokers []string, testFilename string) {

  testConfigs, err := newTestConfigs(testFilename)
  if err != nil {
    logger.Printf("error: %v", err)
    panic(err)
  }
  done := make(chan string)

  for index, testConfig := range testConfigs.TestCfgs {
    logger.Println("Running test", index, testConfig.Topic)
    go runTest(brokers, &testConfigs.TestCfgs[index], done)
  }

  for index, _ := range testConfigs.TestCfgs {
    topic := <-done
    logger.Println("Finished test", index, topic)
  }

}

func runTest(brokers []string, t *TestConfig, done chan string) {

  var batchSize = t.Count/10
  var wg sync.WaitGroup
  var successes = 0
  var errors = 0

  kafkaProducer, err := newKafkaProducer(brokers)
  if err != nil {
    logger.Printf("error: %v", err)
    return
  }

  defer func() {
    kafkaProducer.Producer.AsyncClose()
    done <- t.Topic
  }()

  go func() {
    for err := range kafkaProducer.Producer.Errors() {
        logger.Println(err)
        errors++
        wg.Done()
    }
  }()

  go func() {
    for range kafkaProducer.Producer.Successes() {
        successes++
        wg.Done()
    }
  }()

  message_map := make(map[string]string)

  message_map["logger"] = "ih-services-kafka-util-test"
  message_map["nodeid"] = "0"
  message_map["nodetype"] = "fake"
  message_map["logsource"] = "console"
  message_map["component"] = t.Component
  message_map["subsystem"] = "ih-services"
  message_map["ssinst"] = "007"
  message_map["dc"] = "node.fake.vci"
  message_map["host"] = t.Component
  message_map["message"] = t.Message

  dayOffsets := strings.Split(t.Dayoffsets, ",") 

  for offsetIndex, dayOffset := range dayOffsets {

    daysOffsetInt, err := strconv.Atoi(dayOffset)
    if err != nil {
      logger.Println(err)
      continue
    }

    offsetstr := time.Now().AddDate(0, 0, -daysOffsetInt).Format(time.RFC3339)
    message_map["@timestamp"] = offsetstr

    wg.Add(t.Count)

    for i := 0; i < t.Count; i++ {
      message_map["message"] = t.Message + " " + strconv.Itoa(offsetIndex) + " :: " + strconv.Itoa(i) 
      messagebytes, err := json.Marshal(message_map)
      if err != nil {
        logger.Println(err)
        continue
      }
 
      kafkaProducer.sendMessage(t.Topic, string(messagebytes[:]))
      if t.Period > 0 {
        time.Sleep(time.Duration(t.Period))
      }
      if i % batchSize == 0 {
        logger.Printf("topic=%s\tdayOffset=%s\tcount=%d\tpercent=%.0f%%\n", t.Topic, dayOffset, i, (float32(i)/float32(t.Count)*100))
      }

    }
  }
  
  wg.Wait()
  logger.Printf("Finished topic (%s) success count: (%d) error count: (%d)\n", t.Topic, successes, errors)

  if *showMetrics {
    metrics.WriteOnce(kafkaProducer.Config.MetricRegistry, os.Stderr)
  }
}

func (kafkaProducer *KafkaProducer) sendMessage(topic string, messagestr string)  {
  message := &sarama.ProducerMessage{Topic: topic, Partition: -1}
  message.Value = sarama.StringEncoder(messagestr)
  kafkaProducer.Producer.Input() <- message
}