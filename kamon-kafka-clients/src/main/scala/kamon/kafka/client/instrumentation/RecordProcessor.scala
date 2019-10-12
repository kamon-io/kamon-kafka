package kamon.kafka.client.instrumentation

import java.time.Instant

import kamon.Kamon
import kamon.context.Context
import kamon.kafka.client.Client
import kamon.trace.Span
import org.apache.kafka.clients.consumer.ConsumerRecords

import scala.collection.mutable

object RecordProcessor {

  import scala.collection.JavaConverters._

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

      records.iterator().asScala.foreach{ record =>
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

          // Key could be optional ... see tests
          Option(record.key()).foreach(k => spanBuilder.tag("kafka.key", record.key().toString))

          if (Client.followStrategy)
            spanBuilder.asChildOf(sendingContext.get(Span.Key))
          else
            spanBuilder.link(sendingContext.get(Span.Key), Span.Link.Kind.FollowsFrom)

          // Link new span also to polling span
          spanBuilder.link(pollSpan, Span.Link.Kind.FollowsFrom)

        val span = if(Client.useDelayedSpans)
            // Kafka's timestamp is expressed in millis, convert to nanos => this might spoil precision here ...
            spanBuilder.delay(Kamon.clock().toInstant(record.timestamp() * 100 * 100)).start(startTime)
          else
            spanBuilder.start(startTime)

          span.finish
        }
      }

    records
  }
}
