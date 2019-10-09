package kamon.kafka.client.instrumentation

import java.time.Instant

import kamon.Kamon
import kamon.context.Context
import kamon.kafka.Kafka
import kamon.trace.Span
import org.apache.kafka.clients.consumer.ConsumerRecords

import scala.collection.mutable

object RecordProcessor {

  import scala.collection.JavaConverters._

  /**
    * Inject Context into Records
    */
  def process[V, K](startTime: Instant, records: ConsumerRecords[K, V]): ConsumerRecords[K, V] = {
    if (!records.isEmpty) {

      val consumerSpansForTopic = new mutable.LinkedHashMap[String, Span]()

      records.partitions().asScala.foreach(partition => {
        val topic = partition.topic

        records.records(partition).asScala.foreach(record => {
          val header = Option(record.headers.lastHeader("kamon-context"))

          val sendingContext = header.map { h =>
            ContextSerializationHelper.fromByteArray(h.value())
          }.getOrElse(Context.Empty)

          val span = consumerSpansForTopic.getOrElseUpdate(topic, {
            val spanBuilder = Kamon.consumerSpanBuilder("poll", "kafka.consumer")
              .tag("component", "kafka.consumer")
              .tag("kafka.partition", partition.partition)
              .tag("kafka.topic", topic)
              .tag("kafka.offset", record.offset)

            // Key could be optional ... see tests
            Option(record.key()).foreach(k => spanBuilder.tag("kafka.key", record.key().toString))

            if (Kafka.followStrategy)
              spanBuilder.asChildOf(sendingContext.get(Span.Key))
            else
              spanBuilder.link(sendingContext.get(Span.Key), Span.Link.Kind.FollowsFrom)

            spanBuilder.start(startTime)
          })
        })
      })

      consumerSpansForTopic.values.foreach(_.finish)
    }
    records
  }
}
