package play.modules.kafka.consumer

import kafka.consumer.Consumer
import kafka.consumer.ConsumerConfig
import kafka.consumer.ConsumerConnector
import play.modules.kafka.prop
import play.modules.kafka.prop._

private[consumer] trait ConsumerConfiguration {

  // Configuration for connecting to Kafka as a consumer.

  // blah
  val KafkaTopic = prop string "kafka.consumer.topic" mandatory

  // blah
  val GroupId = prop string "kafka.consumer.groupid" mandatory

  // blah
  val ZkConnect = prop string "kafka.zk.connect.string" mandatory

  // blah
  val NumStreams = prop int "kafka.consumer.num.streams" default 2

  // blah
  val SocketTimeout = prop int "kafka.consumer.socket.timeout.ms" default 1000

  // blah
  val CommitInterval = prop int "kafka.consumer.commit.ms" default 1000

  // blah
  val ConsumerTimeout = prop int "kafka.consumer.timeout.ms" default 30000

  // Other configuration data

  // blah
  val NumberOfWorkers = prop int "play-kafka.consumer.num-workers" mandatory

  // Other setup

  // blah
  def makeConnector(): ConsumerConnector = {
    val consumerProps = prop (
      "zk.connect" -> ZkConnect,
      "socket.timeout.ms" -> SocketTimeout.toString,
      "socket.timeout.ms" -> SocketTimeout.toString,
      "autocommit.interval.ms" -> CommitInterval.toString,
      "consumer.timeout.ms" -> ConsumerTimeout.toString,
      "groupid" -> GroupId)
    Consumer.create(new ConsumerConfig(consumerProps))
  }
}
