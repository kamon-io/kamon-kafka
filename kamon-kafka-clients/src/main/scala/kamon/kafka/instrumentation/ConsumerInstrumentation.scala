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

import java.nio.ByteBuffer

import kamon.Kamon
import kamon.context.Context
import kamon.kafka.client.instrumentation.advisor.Advisors.PollMethodAdvisor
import kamon.trace.Span
import kanela.agent.scala.KanelaInstrumentation
import org.apache.kafka.clients.consumer.ConsumerRecords

import scala.collection.mutable

class ConsumerInstrumentation extends KanelaInstrumentation {

  /**
    * Instruments org.apache.kafka.clients.consumer.KafkaConsumer::poll(long)
    */
  forTargetType("org.apache.kafka.clients.consumer.KafkaConsumer") { builder =>
    builder
      .withAdvisorFor(method("poll").and(withArgument(0, classOf[Long])), classOf[PollMethodAdvisor])
      .build()
  }
}

object RecordProcessor {

  /**
    * Inject Context into Records
    */
  def process[V, K](records: ConsumerRecords[K, V]): ConsumerRecords[K, V] = {
    if (!records.isEmpty) {

      lazy val instant = Kamon.clock.instant()

      val consumerSpansForTopic = new mutable.LinkedHashMap[String, Span]()

      records.partitions().forEach(partition => {
        val topic = partition.topic

        records.records(partition).forEach(record => {
          val header = Option(record.headers.lastHeader("kamon-context"))

          val currentContext = header.map(h => Kamon.contextCodec.Binary.decode(ByteBuffer.wrap(h.value))).getOrElse(Context.Empty)

          val span = consumerSpansForTopic.getOrElseUpdate(topic, {
            Kamon.buildSpan("poll")
              .asChildOf(currentContext.get(Span.ContextKey))
              .withMetricTag("span.kind", "consumer")
              .withTag("kafka.partition", partition.partition)
              .withTag("kafka.topic", topic)
              .withFrom(instant)
              .start()
          })

          val ctx = currentContext.withKey(Span.ContextKey, span)
          record.headers.add("kamon-context", Kamon.contextCodec.Binary.encode(ctx).array())
        })
      })

      consumerSpansForTopic.values.foreach(_.finish)
    }
    records
  }
}