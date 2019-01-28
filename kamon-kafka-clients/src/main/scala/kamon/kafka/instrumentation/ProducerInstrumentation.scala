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

package kamon.kafka.instrumentation

import kamon.context.Storage.Scope
import kamon.kafka.client.instrumentation.advisor.Advisors.SendMethodAdvisor
import kamon.trace.Span
import kanela.agent.scala.KanelaInstrumentation
import org.apache.kafka.clients.producer.{Callback, RecordMetadata}

class ProducerInstrumentation extends KanelaInstrumentation {

  /**
    * Instruments "org.apache.kafka.clients.producer.KafkaProducer::Send()
    */
  forTargetType("org.apache.kafka.clients.producer.KafkaProducer") { builder =>
    builder
      .withAdvisorFor(method("send").and(takesArguments(2)), classOf[SendMethodAdvisor])
      .build()
  }
}

/**
  * Producer Callback Wrapper
  */
final class ProducerCallback(callback: Callback, scope: Scope) extends Callback {
  override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
    val span = scope.context.get(Span.ContextKey)
    if(exception != null) span.addError(exception.getMessage, exception)
    try if(callback != null) callback.onCompletion(metadata, exception)
    finally {
      span.finish()
      scope.close()
    }
  }
}
