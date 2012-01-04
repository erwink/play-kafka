package play.modules.kafka.consumer

import akka.actor._
import akka.actor.Actor._
import akka.actor.FSM._
import akka.dispatch.Futures
import akka.routing._
import akka.routing.Routing._
import java.util.concurrent.CountDownLatch
import kafka.consumer.KafkaMessageStream
import scala.actors.Future
import kafka.message.Message
import Types._
import InternalTypes._

private[consumer] object InternalTypes {
  case class ProcessStreams(streams: List[KafkaMessageStream])
  case class ProcessOneMessage(stream: KafkaMessageStream)

  case class WorkerResponse(stream: KafkaMessageStream, next: Next)

  sealed trait State
  case object Waiting extends State
  case object Processing extends State
  case object Finished extends State

  sealed trait Data
  case class Config(numberOfWorkers: Int) extends Data
  case class WorkerData(router: ActorRef, workers: List[ActorRef]) extends Data
}

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
          Futures awaitAll {
            workers map { _ !!! PoisonPill }
          }

          // Stop the router as well.
          router !! PoisonPill

          // Countown the latch. This will signal that the job is done.
          latch.countDown()

          // Go to state Finsished. No data is needed.
          goto(Finished)
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
    case ProcessOneMessage(stream) => self reply processOneMessage(stream)
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
}

// Stolen from Akka's FSM.scala (not available in Akka 1.0)
object -> {
  def unapply[S](in: (S, S)) = Some(in)
}
