package play.modules.kafka.consumer

import akka.actor._
import akka.actor.Actor._
import play.jobs.Job
import java.util.concurrent.CountDownLatch
import kafka.message.Message
import kafka.consumer.KafkaMessageStream

abstract class AbstractConsumerJob extends Job {

  /**
   *
   */
  def onMessage(message: Message): Result

  override def doJob() {

    // Set up the configuration data.
    val config: Config = null

    // Initialize the count down latch which will be used to block this thread
    // until the consumer FSM is done.
    val latch = new CountDownLatch(1)

    // Get a list of streams from Kafka API.
    val streams: List[KafkaMessageStream] = null

    // Create the consumer FSM.
    val consumer = actorOf(new ConsumerFSM(config, latch)(onMessage _)).start

    // Tell the consumer FSM to process the streams.
    consumer ! ProcessStreams(streams)

    // Block until the consumer FSM completes.
    latch.await()
  }
}