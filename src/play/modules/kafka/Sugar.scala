package play.modules.kafka

import java.util.Properties
import scala.collection.TraversableLike

/**
 * Sugar to make using Java API for Play nicer.
 */
object prop {

  def string(name: String): Option[String] = {
    Option(play.configuration(name))
  }

  def boolean(name: String): Option[Boolean] = {
    string(name) map ("true" == _.toLowerCase)
  }

  def int(name: String): Option[Int] = {
    string(name) map (Integer parseInt _)
  }

  implicit def toDefaultable[T](o: Option[T]): D[T] = new D(o)

  def apply(pairs: (String, String)*): Properties = {
    val props = new Properties
    pairs foreach { pair => props.put(pair._1, pair._2) }
    props
  }

  class ConfigurationException(message: String, args: Any*)
    extends RuntimeException(message format args)
}

case class D[T](option: Option[T]) {
  def default(t: T): T = option getOrElse t
  def mandatory: T = option match {
    case Some(t) => t
    case None => throw new prop.ConfigurationException(
      "mandatory param [%s] not defined")
  }
}