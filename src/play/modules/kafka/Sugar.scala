package play.modules.kafka

import java.util.Properties
import scala.collection.TraversableLike

/**
 * Sugar to make using Java API for Play nicer.
 */
object config {

  def string(name: String): Option[String] = {
    Option(play.configuration(name))
  }

  def boolean(name: String): Option[Boolean] = {
    string(name) map ("true" == _.toLowerCase)
  }

  def int(name: String): Option[Int] = {
    string(name) map (Integer parseInt _)
  }

  implicit def toProp[T](option: Option[T]): prop[T] = new prop(option)

  def properties(pairs: (String, String)*): Properties = {
    val props = new Properties
    pairs foreach { pair => props.put(pair._1, pair._2) }
    props
  }

  class ConfigurationException(message: String, args: Any*)
    extends RuntimeException(message format args)
}

case class prop[T](option: Option[T]) {
  def default(t: T): T = option getOrElse t
  def or(t: T) = default(t)

  def mandatory: T = option match {
    case Some(t) => t
    case None => throw new config.ConfigurationException(
      "mandatory param [%s] not defined")
  }
  def ! = mandatory
}

class safe[A](thing: () => A) {
  def or(a: A): A = {
    try {
      thing.apply()
    } catch {
      case error => a
    }
  }
}

object safely {
  def apply(f: => Unit) {
    val wrapped = () => f
    new safe(wrapped) or Unit
  }

  def apply[A](f: => A): safe[A] = {
    val wrapped = () => f
    new safe(wrapped)
  }
}