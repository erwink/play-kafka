package play.modules.kafka.consumer

import akka.actor._
import Actor._
import FSM._
import akka.routing._
import akka.routing.Routing._
import kafka.consumer.KafkaMessageStream
import kafka.message.Message
import scala.actors.Future
import akka.dispatch.Futures
import java.util.concurrent.CountDownLatch

case class ProcessStreams(streams: List[KafkaMessageStream])
case class ProcessOneMessageFromStream(stream: KafkaMessageStream)
case class DoneProcessingMessage(stream: KafkaMessageStream)

sealed trait Result
case object Success extends Result
case object Failure extends Result

sealed trait State
case object Waiting extends State
case object Processing extends State
case object Done extends State

sealed trait Data
case class Config(numberOfWorkers: Int) extends Data
case class ConsumerData(router: ActorRef, workers: List[ActorRef]) extends Data

class ConsumerFSM(config: Config, latch: CountDownLatch)(f: Message => Result)
    extends Actor with FSM[State, Data] {

  startWith(Waiting, config)

  when(Waiting) {
    case Event(ProcessStreams(streams), config: Config) =>
      val data = initData(config)
      for (stream <- streams) data.router ! ProcessOneMessageFromStream(stream)
      goto(Processing) using data
  }

  when(Processing) {
    case Event(DoneProcessingMessage(stream), ConsumerData(router, workers)) =>
      router ! ProcessOneMessageFromStream(stream)
      stay

    case Event(Failure, ConsumerData(router, workers)) =>
      Futures awaitAll { workers map { _ !!! PoisonPill } }
      router !! PoisonPill
      latch.countDown()
      goto(Done)
  }

  initialize

  private def initData(config: Config): ConsumerData = {
    // Create the workers.
    val workers = List.fill(config.numberOfWorkers) {
      actorOf(new ConsumerWorker(f)).start()
    }

    // Wrap them with a load-balancing router.
    val router = Routing.loadBalancerActor(
      new SmallestMailboxFirstIterator(workers)).start()

    new ConsumerData(router, workers)
  }
}

class ConsumerWorker(f: Message => Result) extends Actor {

  override def receive = {
    case ProcessOneMessageFromStream(stream) =>
      val result = processOneMessage(stream)
      result match {
        case Success => self reply DoneProcessingMessage(stream)
        case Failure => self reply Failure
      }
  }

  private def processOneMessage(stream: KafkaMessageStream): Result = {
    val streamIterator = stream.iterator
    val hasNext = streamIterator.hasNext()
    hasNext match {
      case true =>
        val message = streamIterator.next()
        processMessage(message)
      case false =>
        Failure
    }
  }

  private def processMessage(message: Message): Result = {
    try {
      f(message)
    } catch {
      case throwable: Throwable => Failure
    }
  }
}

// Stolen from Akka's FSM.scala (not available in v1)
object -> {
  def unapply[S](in: (S, S)) = Some(in)
}
