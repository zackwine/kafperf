package main

import (
  "github.com/Shopify/sarama"
  "fmt"
)

type KafkaMonitor struct {
  brokerList []string
  brokers []*sarama.Broker
  config sarama.Config
  kafkaVersion sarama.KafkaVersion
  client sarama.Client

  topicMetadata map[string]*sarama.TopicMetadata
  consumerGroups []string
  kafkaGroups map[string]*KafkaGroupMetadata
}

type KafkaGroupMetadata struct {
  topics map[string]struct{}
}

type KafkaGroupPartitionOffset struct {
  partitionID   int32
  leader        int32
  groupOffset   int64
  newestOffset  int64
}

type KafkaTopicOffsets struct {
  topicName     string
  offsets       []*KafkaGroupPartitionOffset
}

type KafkaGroupOffsets struct {
  groupName     string
  topics        []*KafkaTopicOffsets
}

func NewKafkaMonitor(brokers []string) (*KafkaMonitor, error) {
  var err error
  k := &KafkaMonitor{}
  k.brokerList = brokers

  k.kafkaVersion = defaultKafkaVersion

  k.config = *sarama.NewConfig()
  k.config.Producer.Return.Successes = true
  k.config.Producer.Partitioner = sarama.NewHashPartitioner
  k.config.Version = k.kafkaVersion

  k.client, err = sarama.NewClient(k.brokerList, &k.config)
  if err != nil {
    return nil, err
  }

  k.topicMetadata = make(map[string]*sarama.TopicMetadata)
  k.kafkaGroups = make(map[string]*KafkaGroupMetadata)

  return k, err
}

func (kafkaMonitor *KafkaMonitor) ConnectBrokers() error {
  for _, brokerAddr := range kafkaMonitor.brokerList {
    broker := sarama.NewBroker(brokerAddr)
    kafkaMonitor.brokers = append(kafkaMonitor.brokers, broker)
    if ok, _ := broker.Connected(); ok {
      continue
    }
    broker.Open(&kafkaMonitor.config)
    // Verify broker is connected
    connected, err := broker.Connected()
    if err != nil {
      return err
    }
    if !connected {
      return fmt.Errorf("failed to connect broker %#v", broker.Addr())
    }
  }

  return nil
}

func (kafkaMonitor *KafkaMonitor) Close() {
  for _, broker := range kafkaMonitor.brokers {
    broker.Close()
  }
  kafkaMonitor.client.Close()
}

func DumpKafkaState(brokers []string) {
  monitor, err := NewKafkaMonitor(brokers)
  if err != nil {
    logger.Printf("Failed create kafka monitor: %v", err)
    return
  }
  defer func() {
    monitor.Close()
  }()

  logger.Printf("Getting Topic Metadata...")
  metadata, err := monitor.GetTopicMetaData(nil)
  if err != nil {
    logger.Printf("Failed to get metadata: %v", err)
    return
  }
  for _, topicMetadata := range metadata.Topics {
    var errStr string
    if topicMetadata.Name == "__consumer_offsets" {
      continue
    }
    if topicMetadata.Err == sarama.ErrNoError {
      errStr = "None"
    }else{
      errStr = topicMetadata.Err.Error()
    }
    logger.Printf("topic=(%s) error=(%s)\n", topicMetadata.Name, errStr)
    for _, partition := range topicMetadata.Partitions {
      logger.Printf("\tpartition: %d, leader: %d, replicas: %v, ISR: %v", partition.ID, partition.Leader, partition.Replicas, partition.Isr)
    }
  }

  logger.Printf("Getting Get Group Metadata...")
  groups, err := monitor.GetGroupsMetadata()
  if err != nil {
    logger.Printf("Failed to get GetGroupsMetadata: %v", err)
    return
  }
  logger.Printf("Groups: %v", groups)

  logger.Printf("Getting Get Group Offsets...")
  offsets, err := monitor.getGroupOffsets(nil)
  if err != nil {
    logger.Printf("Failed to get GroupOffsets: %v", err)
    return
  }
  for _, offset := range offsets {
    offset.print()
  }
}

func (kafkaMonitor *KafkaMonitor) GetGroupOffsets(groups []string) (offsets map[string]*KafkaGroupOffsets, err error) {
  _, err = kafkaMonitor.GetTopicMetaData(nil)
  if err != nil {
    logger.Printf("Failed to get GetTopicMetaData: %v", err)
    return
  }

  _, err = kafkaMonitor.GetGroupsMetadata()
  if err != nil {
    logger.Printf("Failed to get GetGroupsMetadata: %v", err)
    return
  }

  offsets, err = kafkaMonitor.getGroupOffsets(groups)

  return
}

func (kgo *KafkaGroupOffsets) print() {
  logger.Printf("%s consumer group topics (%d)", kgo.groupName, len(kgo.topics))
  for _, topic := range kgo.topics {
    logger.Printf("  %s topic partitions (%d)", topic.topicName, len(topic.offsets))
    for _, offset := range topic.offsets {
      logger.Printf("    partitionID: %d, leader: %d, newestOffset: %d, groupOffset: %d, lag: %d",
        offset.partitionID, offset.leader, offset.newestOffset, offset.groupOffset, (offset.newestOffset-offset.groupOffset))
    }
  }
}

// Assumes GetTopicMetaData and GetGroupsMetadata have been called recently to populate KafkaMonitor struct
func (kafkaMonitor *KafkaMonitor) getGroupOffsets(groups []string) (offsets map[string]*KafkaGroupOffsets, err error) {

  offsets = make(map[string]*KafkaGroupOffsets)

  if groups == nil {
    groups = kafkaMonitor.consumerGroups
  }

  for _, group := range groups {
    groupOffsets := &KafkaGroupOffsets{
      groupName: group,
      topics: make([]*KafkaTopicOffsets, 0),
    }
    // For a given group loop over topics it references.
    for topic, _ := range kafkaMonitor.kafkaGroups[group].topics {
      topicMeta := kafkaMonitor.topicMetadata[topic]
      topicOffsets := &KafkaTopicOffsets{
        topicName: topic,
        offsets: make([]*KafkaGroupPartitionOffset, 0),
      }
      // For a given topic loop over partitions
      for _, partition := range topicMeta.Partitions {
        // Get offsets for each group/topic/partition combo
        groupOffset, err := kafkaMonitor.getGroupOffset(topic, group, partition.ID)
        if err != nil {
          logger.Printf("Failed to get group offset: %v", err)
          continue
        }
        newestOffset, err := kafkaMonitor.client.GetOffset(topic, partition.ID, sarama.OffsetNewest)
        if err != nil {
          logger.Printf("Failed to get topic offset: %v", err)
          continue
        }
        groupPartitionOffset := &KafkaGroupPartitionOffset{
          partitionID:  partition.ID,
          leader:       partition.Leader,
          groupOffset:  groupOffset,
          newestOffset: newestOffset,
        }
        topicOffsets.offsets = append(topicOffsets.offsets, groupPartitionOffset)
      }
      groupOffsets.topics = append(groupOffsets.topics, topicOffsets)
    }
    offsets[group] = groupOffsets
  }
  return
}

func (kafkaMonitor *KafkaMonitor) getGroupOffset(topic string, group string, partition int32) (grpOffset int64, retErr error) {
  var offsetManager sarama.OffsetManager
  var pom sarama.PartitionOffsetManager
  grpOffset = -1

  if offsetManager, retErr = sarama.NewOffsetManagerFromClient(group, kafkaMonitor.client); retErr != nil {
    logger.Printf("Failed to get NewOffsetManagerFromClient: %v", retErr)
    return
  }
  defer offsetManager.Close()

  pom, retErr = offsetManager.ManagePartition(topic, partition)
  if retErr != nil {
    logger.Printf("Failed to manage partition group=%s topic=%s partition=%d err=%v", group, topic, partition, retErr)
    return
  }
  defer pom.Close()

  grpOffset, _ = pom.NextOffset()
  //logger.Printf("Found group offset for group=%s topic=%s partition=%d offset=%v", group, topic, partition, grpOffset)
  return
}

// Create a map of group to broker and owned topics from description for a single broker
func (kafkaMonitor *KafkaMonitor) getGroupsDescription(broker *sarama.Broker, groups []string) {

  req := &sarama.DescribeGroupsRequest{groups}
  groupDescs, err := broker.DescribeGroups(req)
  if err != nil {
    logger.Printf("err: %v", err)
    return
  }

  for _, groupDesc := range groupDescs.Groups {
    //logger.Printf("groupName: %s, groupDescs: %+v", groupDesc.GroupId, groupDesc)

    kafkaGroupMeta := &KafkaGroupMetadata{}
    kafkaGroupMeta.topics = make(map[string]struct{})

    for _, memberDesc := range groupDesc.Members {
      //logger.Printf("\tmemberDescs: %+v", memberDesc)
      memberMetadata, err := memberDesc.GetMemberMetadata()
      if err != nil {
        logger.Printf("err: %v", err)
        continue
      }
      for _, topic := range memberMetadata.Topics {
        kafkaGroupMeta.topics[topic] = struct{}{} // Use map keys as a unique list
      }
    }

    kafkaMonitor.kafkaGroups[groupDesc.GroupId] = kafkaGroupMeta
  }

  return 
}

// Create a map of group to broker and owned topics from description for all brokers
func (kafkaMonitor *KafkaMonitor) GetGroupsMetadata() ([]string, error) {
  var groups []string
  err := kafkaMonitor.ConnectBrokers()
  if err != nil {
    logger.Printf("err: %v", err)
    return nil, err
  }
  req := &sarama.ListGroupsRequest{}
  kafkaMonitor.consumerGroups = nil

  for _, broker := range kafkaMonitor.brokers {
    groupsResp, err2 := broker.ListGroups(req)
    if err2 != nil {
      logger.Printf("err2: %v", err2)
      return nil, err
    }
    groups = nil // Empty slice
    for groupName, _ := range groupsResp.Groups {
      //logger.Printf("groupName: %s, groupType: %s", groupName, groupType)
      groups = append(groups, groupName)
      kafkaMonitor.consumerGroups = append(kafkaMonitor.consumerGroups, groupName)
    }
    kafkaMonitor.getGroupsDescription(broker, groups)
  }
  return groups, nil
}

func (kafkaMonitor *KafkaMonitor) GetTopicMetaData(topics []string) (metaDataResp *sarama.MetadataResponse, err error) {
  metaDataResp = nil

  err = kafkaMonitor.ConnectBrokers()
  if err != nil {
    logger.Printf("Failed to connect to brokers: %v", err)
    return
  }
  if topics == nil {
    topics, err = kafkaMonitor.GetTopics()
    if err != nil {
      logger.Printf("Failed to get topics: %v", err)
      return
    }
  }
  req := &sarama.MetadataRequest{Topics:topics}
  metaDataResp, err = kafkaMonitor.brokers[0].GetMetadata(req)
  kafkaMonitor.topicMetadata = make(map[string]*sarama.TopicMetadata)
  for _, topicMeta := range metaDataResp.Topics {
    kafkaMonitor.topicMetadata[topicMeta.Name] = topicMeta
  }

  return 
}

func (kafkaMonitor *KafkaMonitor) GetTopics() ([]string, error) {
  return  kafkaMonitor.client.Topics()
}

