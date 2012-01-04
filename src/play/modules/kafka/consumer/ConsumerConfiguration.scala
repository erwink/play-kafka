package play.modules.kafka.consumer

import play.modules.kafka.config
import kafka.consumer.Consumer
import kafka.consumer.ConsumerConfig

private[consumer] trait ConsumerConfiguration {

  // Configuration for connecting to Kafka as a consumer.

  val KafkaTopic = config string "kafka.consumer.topic"
  val NumberOfStreams = config int "kafka.consumer.num.streams"
  val ZookeeperConnectString = config string "kafka.zk.connect.string"
  val SocketTimeout = config int "kafka.consumer.socket.timeout.ms"
  val AutoCommitInterval = config int "kafka.consumer.autocommit.ms"
  val ConsumerTimeout = config int "kafka.consumer.timeout.ms"
  val GroupId = config string "kafka.consumer.groupid"

  lazy val connector = {
    val consumerProps = config.properties(
      "zk.connect" -> ZookeeperConnectString,
      "socket.timeout.ms" -> SocketTimeout.toString,
      "socket.timeout.ms" -> SocketTimeout.toString,
      "autocommit.interval.ms" -> AutoCommitInterval.toString,
      "consumer.timeout.ms" -> ConsumerTimeout.toString,
      "groupid" -> GroupId)
    Consumer.create(new ConsumerConfig(consumerProps))
  }

  // Other configuration data

  val NumberOfWorkers = config int "play-kafka.consumer.num-workers"
}