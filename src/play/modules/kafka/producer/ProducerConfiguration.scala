package play.modules.kafka.producer

import play.modules.kafka.config
import play.modules.kafka.config._

private[producer] trait ProducerConfiguration {

  // Configuration for connecting to Kafka as a producer.

  val KafkaTopic = config string "kafka.consumer.topic" !
  val GroupId = config string "kafka.consumer.groupid" !
  val ZkConnect = config string "kafka.zk.connect.string" !
  val NumStreams = config int "kafka.consumer.num.streams" or 1
  val SocketTimeout = config int "kafka.consumer.socket.timeout.ms" or 1000
  val CommitInterval = config int "kafka.consumer.commit.ms" or 1000
  val ConsumerTimeout = config int "kafka.consumer.timeout.ms" or 30000
}