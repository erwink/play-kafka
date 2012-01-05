package play.modules.kafka.consumer

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Actor.actorOf
import akka.actor.FSM
import akka.actor.PoisonPill
import akka.actor.actorRef2Scala
import akka.dispatch.Futures
import akka.routing.Routing
import akka.routing.SmallestMailboxFirstIterator
import java.util.concurrent.CountDownLatch
import kafka.consumer.KafkaMessageStream
import kafka.message.Message
import types.MessageResult
import play.Logger
import org.apache.commons.lang.exception.ExceptionUtils

case class ProcessStreams(streams: List[KafkaMessageStream])
case class ProcessOneMessage(stream: KafkaMessageStream)

case class WorkerResponse(stream: KafkaMessageStream, next: Next) {
  override def toString() = "WorkerResponse: " + next
}

sealed trait State
case object Waiting extends State
case object Processing extends State
//case object Finished extends State

sealed trait Data
case class Config(numberOfWorkers: Int) extends Data
case class WorkerData(router: ActorRef, workers: List[ActorRef]) extends Data

private[consumer] class ConsumerFSM(
  config: Config, latch: CountDownLatch)(onMessage: Message => MessageResult)
    extends Actor with FSM[State, Data] {

  startWith(Waiting, config)

  when(Waiting) {
    case Event(ProcessStreams(streams), config: Config) =>
      // Initialize the WorkerData from the configuration.
      val workerData = initData(config)

      // Send each stream off through the router to start.
      for (stream <- streams) workerData.router ! ProcessOneMessage(stream)

      // Go to state Processing using the current WorkerData.
      goto(Processing) using workerData
  }

  when(Processing) {
    case Event(WorkerResponse(stream, next), WorkerData(router, workers)) =>
      next match {
        // The normal case. We should keep going.
        case More =>
          // Use the router to tell a worker to process the returned stream.
          router ! ProcessOneMessage(stream)

          // Stay in this state and maintain the same worker data.
          stay using WorkerData(router, workers)

        // A worker told us to stop.
        // We will need to stop all workers and go to Finished state.
        case Stop =>
          // Stop all workers and wait for them all to confirm.
          workers foreach { _ ! PoisonPill }

          // Stop the router as well.
          router ! PoisonPill

          // Countown the latch. This will signal that the job is done.
          latch.countDown()

          // Go to state Finsished. No data is needed.
          goto(Waiting) using config
      }
  }

  initialize

  private def initData(config: Config): WorkerData = {
    // Create the workers.
    val workers = List.fill(config.numberOfWorkers) {
      actorOf(new ConsumerWorker(onMessage)).start()
    }

    // Wrap them with a load-balancing router.
    val router = Routing.loadBalancerActor(
      new SmallestMailboxFirstIterator(workers)).start()

    new WorkerData(router, workers)
  }
}

private[consumer] class ConsumerWorker(
    onMessage: Message => MessageResult) extends Actor {

  override def receive = {
    case ProcessOneMessage(stream) =>
      // This is VERY defensive. Just give it a supervisor...
      self reply safely(stream) { processOneMessage _ }
      Logger.info("I'm still ok!")
  }

  private def processOneMessage(stream: KafkaMessageStream): WorkerResponse = {
    val streamIterator = stream.iterator
    val next = if (streamIterator.hasNext()) {
      val message = streamIterator.next()
      processMessage(message)
    } else {
      Stop
    }
    new WorkerResponse(stream, next)
  }

  private def processMessage(message: Message): Next = {
    try {
      val (current, next) = onMessage(message)
      current match {
        case Done          => Unit
        case TryAgainLater => error("implement producer logic")
      }
      next
    } catch {
      case throwable: Throwable => More
    }
  }

  private def safely(
    stream: KafkaMessageStream)(f: KafkaMessageStream => WorkerResponse): WorkerResponse = {
    try {
      f(stream)
    } catch {
      case error =>
        Logger.error(ExceptionUtils.getStackTrace(error), "got an error!")
        WorkerResponse(stream, Stop)
    }
  }
}

// Stolen from Akka's FSM.scala (not available in Akka 1.0)
object -> {
  def unapply[S](in: (S, S)) = Some(in)
}
