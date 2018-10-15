# Test Environment

A few notes about the test environment

 - Requires docker running on the development machine
 - Launches a few containers that run kafka, zookeeper, and a consumer
 - Creates a topic "kafperf", and creates a consumer for that topic

## Usage

Run the following from the testenv directory to bring up the test environment:
  
    docker-compose up

To clean up the test environment run the following:

    docker-compose down

## Development notes

To launch another container on the same network as above do the following:

    docker run -it -v $PWD:/kafperf --network testenv_default --link testenv_kafka_1 spotify/kafka  bash -i
    cd /kafperf
    /kafperf/xtargets/linux/kafperf -brokers testenv_kafka_1:9092 -dump


Launch a consumer:

    docker run -it -v $PWD:/kafperf --network testenv_default --link testenv_kafka_1 spotify/kafka  bash -i
    /opt/kafka_*/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic kafperf


Launch a producer:

    docker run -it -v $PWD:/kafperf --network testenv_default --link testenv_kafka_1 spotify/kafka  bash -i
    /opt/kafka_*/bin/kafka-console-producer.sh --bootstrap-server kafka:9092 --topic kafperf