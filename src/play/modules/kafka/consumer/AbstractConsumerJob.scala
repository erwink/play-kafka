package play.modules.kafka.consumer

import akka.actor._
import akka.actor.Actor._
import java.util.concurrent.CountDownLatch
import kafka.consumer.ConsumerConnector
import kafka.consumer.KafkaMessageStream
import kafka.message.Message
import play.Logger
import play.jobs.Job
import play.modules.kafka.safely

sealed trait Current
case object Done extends Current
case object TryAgainLater extends Current

sealed trait Next
case object More extends Next
case object Stop extends Next

abstract class AbstractConsumerJob extends Job with ConsumerConfiguration {

  /**
   * Function to be applied to all messages. It is passed the {@link Message} to
   * be processed and should return a result.
   *
   * The result is a pair containing, a {@link Current} result
   * and a {@link Next} instruction. Both have two possible values, and any
   * combination thereof is a valid {@link MessageResult}.
   *
   * <p><ul>{@link Current} can be:
   * <li>{@link Done} signals that we are done with the message. Its processing
   * could have succeeded or permanently failed.</li>
   * <li>{@link TryAgainLater} signals that we want to try that message again
   * later, so it will be put back into the queue.</li></ul>
   *
   * <p><ul>{@link Next} can be:
   * <li>{@link More} signals that we can continue processing. This is the
   * default.</li>
   * <li>{@link Stop} signals that we should stop more messages. This
   * instruction will cause all current threads to stop processing when they
   * have completed their current message. This behavior is helpful if, perhaps,
   * an server is down and we expect all messages to have similar failures.</li>
   * </ul>
   *
   * <p>If a stream times out waiting for a message, the result will be
   * {@code Stop}. The kafka consumer API gets in an undesirable state
   * if a stream times out, so we assume there are no more messages coming for
   * a while, take down the jobs, and start up again later.
   *
   * <p>Any exceptions thrown by this function will be caught and the result
   * will be {@code ExceptionalResult}. This value can be overridden.
   *
   * <p>Note: As implied by the name, this MUST be thread safe as it will be
   * called concurrently by different workers.
   */
  def threadSafeProcessMessage(message: Message): (Current, Next)

  val ExceptionalResult = (Done, More)

  /**
   * Override this method to give a precondition to check before starting
   * the consumer.
   */
  def checkBeforeStart(): Boolean = true

  /*
   * ****************************************************************
   *                PRIVATE IMPLEMENTATION DETAILS
   * ****************************************************************
   */

  private def onMessage(message: Message): (Current, Next) = {
    safely { threadSafeProcessMessage(message) } or ExceptionalResult
  }

  /*
   * The main method of the job. Responsible for checking for readiness,
   * connecting to kafka, starting the consumer FSM, and awaiting completion.
   */
  override final def doJob() {

    if (!checkBeforeStart()) {
      Logger.warn("Startup precondition failed...will try again soon.")
      return
    }

    val config = new Config(NumberOfWorkers)

    // The CountDownLatch will be used to block until the consumer is done.
    val latch = new CountDownLatch(1)

    // Get a list of streams from Kafka API.
    val connector = createConnector()
    val map = connector.createMessageStreams(Map(KafkaTopic -> NumStreams))
    val streams = map(KafkaTopic)

    // Create the consumer, start it, and tell it to process the streams.
    val consumer = actorOf { new ConsumerFSM(config, latch)(onMessage _) }
    consumer.start()
    consumer ! ProcessStreams(streams)

    // Block until the consumer FSM completes.
    latch.await()

    // Commit offsets before shutting down.
    connector.commitOffsets
    connector.shutdown()
  }
}
