package jobs

import kafka.message.Message
import play._
import play.jobs.Every
import play.modules.kafka.consumer.AbstractConsumerJob
import play.modules.kafka.consumer.types.MessageResult
import play.mvc._
import play.modules.kafka.consumer._

@Every("1s")
class LogKafkaMessage extends AbstractConsumerJob {

  override def checkBeforeStart(): Boolean = {
    Logger.info("Checking...")
    true
  }

  override def threadSafeProcessMessage(message: Message): MessageResult = {
    Logger.info("message: " + new String(message.payload.array))
    Done -> More
  }
}
