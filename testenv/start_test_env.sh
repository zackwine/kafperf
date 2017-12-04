#!/bin/sh
set -ex

docker run -d -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=127.0.0.1 --env ADVERTISED_PORT=9092 --name kafkaesque spotify/kafka
sleep 20
docker run --link kafkaesque spotify/kafka bash -c '$KAFKA_HOME/bin/kafka-topics.sh --create --zookeeper kafkaesque:2181 --replication-factor 1 --partitions 5 --topic kafperf'
