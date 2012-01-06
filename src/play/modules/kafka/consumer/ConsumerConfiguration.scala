package play.modules.kafka.consumer

import kafka.consumer.Consumer
import kafka.consumer.ConsumerConfig
import kafka.consumer.ConsumerConnector
import play.modules.kafka.config
import play.modules.kafka.config._

private[consumer] trait ConsumerConfiguration {

  // Configuration for connecting to Kafka as a consumer.

  val KafkaTopic = config string "kafka.consumer.topic" !
  val GroupId = config string "kafka.consumer.groupid" !
  val ZkConnect = config string "kafka.zk.connect.string" !
  val NumStreams = config int "kafka.consumer.num.streams" or 1
  val SocketTimeout = config int "kafka.consumer.socket.timeout.ms" or 1000
  val CommitInterval = config int "kafka.consumer.commit.ms" or 1000
  val ConsumerTimeout = config int "kafka.consumer.timeout.ms" or 30000

  // Other configuration data

  val NumberOfWorkers = config int "play-kafka.consumer.num-workers" or 1

  // Other setup

  // blah
  def makeConnector(): ConsumerConnector = {
    val consumerProps = config properties (
      "zk.connect" -> ZkConnect,
      "socket.timeout.ms" -> SocketTimeout.toString,
      "socket.timeout.ms" -> SocketTimeout.toString,
      "autocommit.interval.ms" -> CommitInterval.toString,
      "consumer.timeout.ms" -> ConsumerTimeout.toString,
      "groupid" -> GroupId)
    Consumer.create(new ConsumerConfig(consumerProps))
  }
}
