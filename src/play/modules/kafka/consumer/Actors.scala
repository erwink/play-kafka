package play.modules.kafka.consumer

import akka.actor._
import akka.actor.Actor._
import akka.config._
import akka.config.Supervision._
import akka.routing._
import akka.routing._
import java.util.concurrent.CountDownLatch
import kafka.consumer.KafkaMessageStream
import kafka.message.Message
import play.Logger
import play.modules.kafka.safely

// Messages sent between Actors.
case class ProcessStreams(streams: List[KafkaMessageStream])
case class ProcessOneMessage(stream: KafkaMessageStream)
case class WorkerResponse(stream: KafkaMessageStream, next: Next) {
  // Define toString to not try to call toString on stream, which is unsafe.
  override def toString() = "WorkerResponse: Next=[%s]".format(next)
}

// States that the FSM can be in.
sealed trait State
case object Waiting extends State
case object Processing extends State
case object Finished extends State

// Data that the FSM can hold.
sealed trait Data
case class Config(numberOfWorkers: Int) extends Data
case class WorkerData(router: ActorRef, workers: List[ActorRef]) extends Data

/**
 *
 */
private class ConsumerFSM(
  config: Config, latch: CountDownLatch)(onMessage: Message => (Current, Next))
    extends Actor with FSM[State, Data] {

  startWith(Waiting, config)

  val strategy = AllForOneStrategy(
    List(classOf[Exception]), // Any error should restart the system.
    maxNrOfRetries = 3, withinTimeRange = 5000) // Not needed, won't restart.

  val supervisor = Supervisor(
    SupervisorConfig(
      strategy,
      Supervise(self, Temporary) :: Nil)).start

  when(Waiting) {
    case Event(ProcessStreams(streams), config: Config) =>
      // Initialize the WorkerData from the configuration.
      val workerData = initWorkers(config)

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

        // A worker told us to stop. We will need to stop all workers.
        case Stop =>
          // Stop all workers and wait for them all to confirm.
          workers foreach { _ ! PoisonPill }

          // Stop the router as well.
          router ! PoisonPill

          // Countown the latch. This will signal that the job is done.
          latch.countDown()

          Logger.info("shutting down normally")

          // Go Finished state. No data is required.
          goto(Finished)
      }
  }

  when(Finished) {
    case any => stay
  }

  initialize

  override def postStop() = {
    Logger.warn("shutdown by supervisor")
    latch.countDown()
  }

  private def initWorkers(config: Config): WorkerData = {
    // Create the workers.
    val workers = List.fill(config.numberOfWorkers) {
      actorOf(new Worker(onMessage)).start()
    }

    workers foreach { worker =>
      worker setLifeCycle Temporary
      supervisor link worker
    }

    // Wrap them with a load-balancing router.
    val router = Routing.loadBalancerActor(
      new SmallestMailboxFirstIterator(workers)).start()

    new WorkerData(router, workers)
  }
}

/**
 *
 */
private class Worker(onMessage: Message => (Current, Next)) extends Actor {

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
    safely {
      val (current, next) = onMessage(message)
      if (current == TryAgainLater) {
        error("implement producer logic")
      }
      next
    } or Stop
  }
}

// Copied from Akka's FSM.scala (not available in Akka 1.0)
object -> { def unapply[S](in: (S, S)) = Some(in) }
