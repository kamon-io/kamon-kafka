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

import java.time.Instant

import kamon.Kamon
import kamon.context.Context
import kamon.kafka.client.Client
import kamon.trace.Span
import org.apache.kafka.clients.consumer.ConsumerRecords

object RecordProcessor {

  import scala.collection.JavaConverters._
  import SpanExtractionSupport._

  def process[V, K](startTime: Instant, clientId: String, groupId: String, records: ConsumerRecords[K, V]): ConsumerRecords[K, V] = {
    if (!records.isEmpty) {
      val spanBuilder = Kamon.consumerSpanBuilder("poll", "kafka.consumer")
        .tagMetrics("kafka.clientId", clientId)
        .tagMetrics("kafka.groupId", groupId)
        .tag("numRecords", records.count())
        .tag("kafka.partitions", records.partitions().asScala.map(_.partition()).mkString(","))
        .tag("kafka.topics", records.partitions().asScala.map(_.topic()).toSet.mkString(","))
      val pollSpan = spanBuilder.start(startTime)
      pollSpan.finish()

      records.iterator().asScala.foreach { record =>
        val header = Option(record.headers.lastHeader("kamon-context"))

        val sendingContext = header.map { h =>
          ContextSerializationHelper.fromByteArray(h.value())
        }.getOrElse(Context.Empty)

        val spanBuilder = Kamon.consumerSpanBuilder("consumed-record", "kafka.consumer")
          .tagMetrics("kafka.topic", record.topic())
          .tagMetrics("kafka.clientId", clientId)
          .tagMetrics("kafka.groupId", groupId)
          .tag("kafka.partition", record.partition())
          .tag("kafka.offset", record.offset)
          .tag("kafka.timestamp", record.timestamp())
          .tag("kafka.timestampType", record.timestampType.name)

        // Key could be optional ... see tests
        Option(record.key()).foreach(k => spanBuilder.tag("kafka.key", record.key().toString))

        if (Client.followStrategy)
          spanBuilder.asChildOf(sendingContext.get(Span.Key))
        else
          spanBuilder.link(sendingContext.get(Span.Key), Span.Link.Kind.FollowsFrom)

        // Link new span also to polling span
        spanBuilder.link(pollSpan, Span.Link.Kind.FollowsFrom)

        val span = if (Client.useDelayedSpans)
          // Kafka's timestamp is expressed in millis, convert to nanos => this might spoil precision here ...
          spanBuilder.delay(Kamon.clock().toInstant(record.timestamp() * 1000 * 1000)).start(startTime)
        else
          spanBuilder.start(startTime)

        // Complete the span and store it on the record (using the HasSpan mixin)
        span.finish
        record.setSpan(span)
      }
    }
    records
  }
}
