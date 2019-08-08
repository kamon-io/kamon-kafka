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

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer

import com.typesafe.config.{Config, ConfigFactory}
import kamon.Kamon
import kamon.context.BinaryPropagation.{ByteStreamReader, ByteStreamWriter}
import kamon.context.{BinaryPropagation, Context}
import kamon.kafka.Kafka
import kamon.kafka.client.instrumentation.advisor.Advisors.PollMethodAdvisor
import kamon.trace.{Span, Trace}
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

          val currentContext = header.map{ h =>
            Kamon.defaultBinaryPropagation().read(ByteStreamReader.of(h.value()))
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

          val out = new ByteArrayOutputStream();
          Kamon.defaultBinaryPropagation().write(currentContext.withEntry(Span.Key, span), ByteStreamWriter.of(out));

          record.headers.add("kamon-context", out.toByteArray)
        })
      })

      consumerSpansForTopic.values.foreach(_.finish)
    }
    records
  }
}