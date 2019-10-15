/*
 * =========================================================================================
 * Copyright © 2013-2019 the kamon project <http://kamon.io/>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * =========================================================================================
 */
package kamon.kafka.client.instrumentation

import kamon.trace.Span
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.util.Try

trait HasSpan {
  def maybeSpan: Option[Span]
  def setSpan(span: Span)
}

object HasSpan {
  class Mixin(@transient private var _span: Option[Span] = None) extends HasSpan {
    override def maybeSpan: Option[Span] = _span
    override def setSpan(span: Span): Unit = _span = Some(span)
  }
}

object SpanExtractionSupport {

  implicit class SpanOps[K, V](cr: ConsumerRecord[K, V]) {

    def setSpan(s: Span): Unit =
      Try {
        cr.asInstanceOf[HasSpan]
      }.toOption.foreach(_.setSpan(s))

    def maybeSpan: Option[Span] =
      Try {
        cr.asInstanceOf[HasSpan].maybeSpan
      }.toOption.flatten
  }

}
