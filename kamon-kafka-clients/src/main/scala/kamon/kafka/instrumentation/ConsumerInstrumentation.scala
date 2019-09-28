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

import kamon.Kamon
import kamon.context.Context
import kamon.kafka.Kafka
import kamon.kafka.client.instrumentation.advisor.Advisors.PollMethodAdvisor
import kamon.trace.Span
import kanela.agent.api.instrumentation.InstrumentationBuilder
import org.apache.kafka.clients.consumer.ConsumerRecords

import scala.collection.mutable

class ConsumerInstrumentation extends InstrumentationBuilder {

  /**
    * Instruments org.apache.kafka.clients.consumer.KafkaConsumer::poll(long)
    */
  onType("org.apache.kafka.clients.consumer.KafkaConsumer")
    .advise(method("poll").and(withArgument(0, classOf[Long])), classOf[PollMethodAdvisor])
}

object RecordProcessor {

  import scala.collection.JavaConverters._

  /**
    * Inject Context into Records
    */
  def process[V, K](records: ConsumerRecords[K, V]): ConsumerRecords[K, V] = {
    if (!records.isEmpty) {

      lazy val instant = Kamon.clock.instant()

      val consumerSpansForTopic = new mutable.LinkedHashMap[String, Span]()

      records.partitions().asScala.foreach(partition => {
        val topic = partition.topic

        records.records(partition).asScala.foreach(record => {
          val header = Option(record.headers.lastHeader("kamon-context"))

          val currentContext = header.map{ h =>
            ContextSerializationHelper.fromByteArray(h.value())
          }.getOrElse(Context.Empty)

          val span = consumerSpansForTopic.getOrElseUpdate(topic, {
            val spanBuilder = Kamon.spanBuilder("poll")
              .tag("span.kind", "consumer")
              .tag("kafka.partition", partition.partition)
              .tag("kafka.topic", topic)
              .tag("kafka.offset", record.offset)

            // Key could be optional ... see tests
            Option(record.key()).foreach(k => spanBuilder.tag("kafka.key", record.key().toString))

            if(Kafka.followStrategy) spanBuilder.asChildOf(currentContext.get(Span.Key))
            else {
              val currentSpan = currentContext.get(Span.Key)
              spanBuilder
                .tag("trace.related.trace_id", currentSpan.id.string)
                .tag("trace.related.span_id", currentSpan.trace.id.string)
            }
            spanBuilder.start()
          })

          val serializedCtx = ContextSerializationHelper.toByteArray(currentContext.withEntry(Span.Key, span))
          record.headers.add("kamon-context", serializedCtx)
        })
      })

      consumerSpansForTopic.values.foreach(_.finish)
    }
    records
  }
}