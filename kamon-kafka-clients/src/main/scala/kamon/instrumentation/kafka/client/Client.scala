package kamon.instrumentation.kafka.client

import com.typesafe.config.Config
import kamon.Kamon
import kamon.context.Context
import kamon.instrumentation.context.HasContext
import kamon.trace.Span
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.util.{Failure, Success, Try}

object Client {

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

  /**
    * Convenience method to extract the context from a `ConsumerRecord`.
    * Returns `Context.Empty` if the record does not have the `HasContext` mixin.
    */
  def context(cr: ConsumerRecord[_, _]): Context =
    Try {
      cr.asInstanceOf[HasContext].context
    } match {
      case Success(c) => c
      case Failure(_) => Context.Empty
    }

  protected[kafka] def setContext(cr: ConsumerRecord[_, _], s: Context): Unit =
    Try {
      cr.asInstanceOf[HasContext]
    }.toOption.foreach(_.setContext(s))

  /**
    * Syntactical sugar for Scala
    * @param cr ConsumerRecord[_,_]
    */
  implicit class Syntax(cr: ConsumerRecord[_, _]) {
    def context: Context = Client.context(cr)
    def span: Span = Client.context(cr).get(Span.Key)
    protected[kafka] def setContext(s: Context): Unit = Client.setContext(cr, s)
  }

}
