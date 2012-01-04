package play.modules.kafka
import java.util.Properties

/**
 * Sugar to make using Java API for Play nicer.
 */
private[kafka] object config {
  def string(name: String): String = {
    checkNotNull(play.configuration(name), "[%s] prop is null".format(name))
  }

  def boolean(name: String): Boolean = {
    "true" equals string(name).toLowerCase
  }

  def int(name: String): Int = {
    Integer parseInt string(name)
  }

  def checkNotNull[T](ref: T, message: String = ""): T = {
    if (ref == null) {
      throw new IllegalStateException(message)
    }
    ref
  }

  def properties(pairs: (String, String)*): Properties = {
    val props = new Properties
    pairs foreach { pair => props.put(pair._1, pair._2) }
    props
  }
}