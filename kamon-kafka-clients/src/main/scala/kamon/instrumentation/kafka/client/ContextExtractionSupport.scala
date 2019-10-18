package kamon.instrumentation.kafka.client

import kamon.context.Context
import kamon.instrumentation.context.HasContext
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.util.{Failure, Success, Try}

object ContextExtractionSupport {

  implicit class ContextOps[K, V](cr: ConsumerRecord[K, V]) {

    def setContext(s: Context): Unit =
      Try {
        cr.asInstanceOf[HasContext]
      }.toOption.foreach(_.setContext(s))

    def context: Context =
      Try {
        cr.asInstanceOf[HasContext].context
      } match {
        case Success(c) => c
        case Failure(t) => Context.Empty
      }
  }

}
