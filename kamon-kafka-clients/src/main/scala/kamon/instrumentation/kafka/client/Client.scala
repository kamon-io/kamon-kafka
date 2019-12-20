package kamon.instrumentation.kafka.client

import com.typesafe.config.Config
import kamon.Kamon
import kamon.context.Context
import kamon.instrumentation.context
import kamon.instrumentation.context.HasContext
import kamon.trace.Span
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.util.{Failure, Success, Try}

object Client {

  object Keys {
    val Null = "NULL"
  }

  val basePath = "kamon.instrumentation.kafka.client"

  @volatile var followStrategy: Boolean = followStrategyFromConfig(Kamon.config())
  @volatile var useDelayedSpans: Boolean = useDelayedSpansFromConfig(Kamon.config())

  private def followStrategyFromConfig(config: Config): Boolean =
    Kamon.config.getBoolean(basePath + ".follow-strategy")

  private def useDelayedSpansFromConfig(config: Config): Boolean =
    Kamon.config.getBoolean(basePath + ".use-delayed-spans")

  Kamon.onReconfigure( (newConfig: Config) => {
      followStrategy = followStrategyFromConfig(newConfig)
      useDelayedSpans = useDelayedSpansFromConfig(newConfig)
    }
  )


  protected[kafka] def setContext(cr: ConsumerRecord[_, _], s: Context): Unit =
    Try {
      cr.asInstanceOf[HasContext]
    }.toOption.foreach(_.setContext(s))

  /**
    * Syntactical sugar for Scala
    * @param cr ConsumerRecord[_,_]
    */
  implicit class Syntax(cr: ConsumerRecord[_, _]) {
    def context: Context = cr.asInstanceOf[HasContext].context
    def span: Span = cr.asInstanceOf[HasContext].context.get(Span.Key)
    protected[kafka] def setContext(s: Context): Unit = cr.asInstanceOf[HasContext].setContext(s)
  }

}
