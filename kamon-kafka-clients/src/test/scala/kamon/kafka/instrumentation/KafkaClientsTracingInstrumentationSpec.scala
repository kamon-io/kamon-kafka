/* =========================================================================================
 * Copyright © 2013-2019 the kamon project <http://kamon.io/>
 *
 * Licensed under the Apache License, Version 2.0 (the "License") you may not use this file
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

import com.typesafe.config.ConfigFactory
import kamon.Kamon
import kamon.tag.Lookups._
import kamon.testkit.Reconfigure
import kamon.trace.Span
import net.manub.embeddedkafka.{Consumers, EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar
import org.scalatest.{BeforeAndAfterAll, Matchers, OptionValues, WordSpec}

class KafkaClientsTracingInstrumentationSpec extends WordSpec
  with Matchers
  with Eventually
  with SpanSugar
  with BeforeAndAfterAll
  with EmbeddedKafka
  with Reconfigure
  with OptionValues
  with Consumers
  with TestSpanReporting {

  // increase zk connection timeout to avoid failing tests in "slow" environments
  implicit val defaultConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig.apply(customBrokerProperties = EmbeddedKafkaConfig.apply().customBrokerProperties + ("zookeeper.connection.timeout.ms" -> "20000"))

  implicit val patienceConfigTimeout = timeout(20 seconds)

  "The Kafka Clients Tracing Instrumentation" should {
    "create a Producer Span when publish a message" in new SpanReportingTestScope(reporter) {
      withRunningKafka {

        publishStringMessageToKafka("kamon.topic", "Hello world!!!!!")

        awaitNumReportedSpans(1)

        assertReportedSpan(_.operationName == "send") { span =>
          span.metricTags.get(plain("component")) shouldBe "kafka.producer"
          span.metricTags.get(plain("span.kind")) shouldBe "producer"
          span.tags.get(plain("kafka.key")) shouldBe "unknown-key"
          span.tags.get(plain("kafka.partition")) shouldBe "unknown-partition"
          span.tags.get(plain("kafka.topic")) shouldBe "kamon.topic"
        }

        assertNoSpansReported()
      }
    }

    "create a Producer/Consumer Span when publish/consume a message" in new SpanReportingTestScope(reporter) {

      withRunningKafka {

        publishStringMessageToKafka("kamon.topic", "Hello world!!!")
        consumeFirstStringMessageFrom("kamon.topic") shouldBe "Hello world!!!"

        awaitNumReportedSpans(2)

        assertReportedSpan(_.operationName == "send") { span =>
          span.metricTags.get(plain("component")) shouldBe "kafka.producer"
          span.metricTags.get(plain("span.kind")) shouldBe "producer"
          span.tags.get(plain("kafka.key")) shouldBe "unknown-key"
          span.tags.get(plain("kafka.partition")) shouldBe "unknown-partition"
          span.tags.get(plain("kafka.topic")) shouldBe "kamon.topic"
        }

        assertReportedSpan(_.operationName == "poll") { span =>
          span.metricTags.get(plain("component")) shouldBe "kafka.consumer"
          span.metricTags.get(plain("span.kind")) shouldBe "consumer"
          span.tags.get(plainLong("kafka.partition")) shouldBe 0L
          span.tags.get(plain("kafka.topic")) shouldBe "kamon.topic"
        }
      }
    }

    "create multiple Producer/Consumer Spans when publish/consume multiple messages" in new SpanReportingTestScope(reporter) {

      withRunningKafka {

        publishStringMessageToKafka("kamon.topic", "m1")
        publishStringMessageToKafka("kamon.topic", "m2")
        consumeFirstStringMessageFrom("kamon.topic") shouldBe "m1"
        consumeFirstStringMessageFrom("kamon.topic") shouldBe "m2"

        awaitNumReportedSpans(4)
        val traceIds = reportedSpans.map(_.trace.id.string).distinct
        traceIds should have size 2
        assertReportedSpans(_.trace.id.string == traceIds.head){ spans => spans should have size 2}
        assertReportedSpans(_.trace.id.string == traceIds(1)){ spans => spans should have size 2}
      }
    }

    "create a Producer/Consumer Span when publish/consume a message without follow-strategy and expect a linked span" in new SpanReportingTestScope(reporter) {
      withRunningKafka {

        Kamon.reconfigure(ConfigFactory.parseString("kamon.kafka.follow-strategy = false").withFallback(Kamon.config()))

        publishStringMessageToKafka("kamon.topic", "Hello world!!!")
        consumeFirstStringMessageFrom("kamon.topic") shouldBe "Hello world!!!"

        awaitNumReportedSpans(2)

        var sendingSpan: Option[Span.Finished] = None
        assertReportedSpan(_.operationName == "send") { span =>
          span.metricTags.get(plain("span.kind")) shouldBe "producer"
          span.metricTags.get(plain("component")) shouldBe "kafka.producer"
          span.tags.get(plain("kafka.key")) shouldBe "unknown-key"
          span.tags.get(plain("kafka.partition")) shouldBe "unknown-partition"
          span.tags.get(plain("kafka.topic")) shouldBe "kamon.topic"
          sendingSpan = Some(span)
        }

        assertReportedSpan(_.operationName == "poll") { span =>
          span.metricTags.get(plain("span.kind")) shouldBe "consumer"
          span.metricTags.get(plain("component")) shouldBe "kafka.consumer"
          span.tags.get(plainLong("kafka.partition")) shouldBe 0L
          span.tags.get(plain("kafka.topic")) shouldBe "kamon.topic"
          span.links should have size 1
          val link = span.links.head
          link.trace.id shouldBe sendingSpan.get.trace.id
          link.spanId shouldBe sendingSpan.get.id
        }

        assertNoSpansReported()
      }
    }
  }
}
