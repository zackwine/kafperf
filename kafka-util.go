package main

import (
  "flag"
  "fmt"
  "log"
  "os"
  "strings"
  "runtime"
  "runtime/pprof"
  "github.com/Shopify/sarama"
)

var (
  brokerList  = flag.String("brokers", os.Getenv("KAFKA_PEERS"), "The comma separated list of brokers in the Kafka cluster. You can also set the KAFKA_PEERS environment variable (kafka0:29090,kafka1:29091,kafka2:29092)")
  testFile    = flag.String("testfile", "", "The path to the test file (yaml) defining tests to run.")
  dump        = flag.Bool("dump", false, "Dump kafka state.")
  verbose     = flag.Bool("verbose", false, "Turn on sarama logging to stderr")
  showMetrics = flag.Bool("metrics", false, "Output metrics on successful publish to stderr")
  silent      = flag.Bool("silent", false, "Turn off printing the message's topic, partition, and offset to stdout")
  cpuprofile  = flag.String("cpuprofile", "", "write cpu profile to file")

  logger = log.New(os.Stderr, "", log.LstdFlags)
)

var defaultKafkaVersion sarama.KafkaVersion = sarama.V0_10_0_0

func printUsageAndBail(message string) {
  fmt.Fprintln(os.Stderr, "ERROR:", message)
  fmt.Fprintln(os.Stderr)
  fmt.Fprintln(os.Stderr, "Usage:")
  flag.PrintDefaults()
  os.Exit(64)
}

func main() {
  runtime.GOMAXPROCS(4)
  flag.Parse()

  if *brokerList == "" {
    printUsageAndBail("no -brokers specified. Alternatively, set the KAFKA_PEERS environment variable")
  }

  if *verbose {
    sarama.Logger = logger
  }

  if *cpuprofile != "" {
    f, err := os.Create(*cpuprofile)
    if err != nil {
      logger.Fatal(err)
    }
    pprof.StartCPUProfile(f)
    defer pprof.StopCPUProfile()
  }

  if *dump {
    DumpKafkaState(strings.Split(*brokerList, ","))
  }

  if *testFile != "" {
    runTests(strings.Split(*brokerList, ","), *testFile)
  }

}

