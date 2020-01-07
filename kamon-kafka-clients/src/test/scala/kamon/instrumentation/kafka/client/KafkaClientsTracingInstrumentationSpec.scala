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
package kamon.instrumentation.kafka.client

import com.typesafe.config.ConfigFactory
import kamon.Kamon
import kamon.context.Context
import kamon.instrumentation.kafka.testutil.{DotFileGenerator, SpanReportingTestScope, TestSpanReporting}
import kamon.tag.Lookups._
import kamon.testkit.Reconfigure
import kamon.trace.Span
import net.manub.embeddedkafka.{Consumers, EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.common.serialization.StringDeserializer
import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Matchers, OptionValues, WordSpec}

class KafkaClientsTracingInstrumentationSpec extends WordSpec
  with Matchers
  with Eventually
  with SpanSugar
  with BeforeAndAfter
  with EmbeddedKafka
  with Reconfigure
  with OptionValues
  with Consumers
  with TestSpanReporting {

  // increase zk connection timeout to avoid failing tests in "slow" environments
  implicit val defaultConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig.apply(customBrokerProperties = EmbeddedKafkaConfig.apply().customBrokerProperties + ("zookeeper.connection.timeout.ms" -> "20000"))

  implicit val patienceConfigTimeout = timeout(20 seconds)
  import Client._
  val testTopicName = "kamon.topic"

  "The Kafka Clients Tracing Instrumentation" should {

/*    "provide the consumed-record span to clients" in new SpanReportingTestScope(reporter) {
      withRunningKafka {
        import net.manub.embeddedkafka.Codecs.stringDeserializer

        publishStringMessageToKafka(testTopicName, "Hello world!!!!!")
        val consumedRecord = consumeFirstRawRecord(testTopicName)
        val contextFromRecord = consumedRecord.context
        contextFromRecord should not be Context.Empty

        val spanFromRecord = contextFromRecord.get(Span.Key)
        Kamon.spanBuilder("my-business-processor") //TODO unnecessarry for test
            .asChildOf(spanFromRecord)
            .start
            .finish

        awaitNumReportedSpans(4) // send + poll + consumed-record
        assertReportedSpan(_.operationName == "consumed-record") { span =>
          span.id shouldBe spanFromRecord.id
        }

        //TODO Should just do a poll with 3 messages and assert 1 poll span with 3 consume spans

        DotFileGenerator.dumpToDotFile("client-hasSpan", reportedSpans)
      }
    }*/

    "create a Producer Span when publish a message" in new SpanReportingTestScope(reporter) {
      withRunningKafka {

        publishStringMessageToKafka(testTopicName, "Hello world!!!!!")

        awaitNumReportedSpans(1)

        assertReportedSpan(_.operationName == "send") { span =>
          span.metricTags.get(plain("component")) shouldBe "kafka.producer"
          span.metricTags.get(plain("span.kind")) shouldBe "producer"
          span.tags.get(plain("kafka.topic")) shouldBe testTopicName
          span.tags.get(plain("kafka.key")) shouldBe Client.Keys.Null
          span.tags.get(plain("kafka.partition")) shouldBe Client.Keys.Null
        }

        assertNoSpansReported()
      }
    }

    "create a Producer/Consumer Span when publish/consume a message" in new SpanReportingTestScope(reporter) {

      withRunningKafka {

        import net.manub.embeddedkafka.Codecs.stringDeserializer

        publishStringMessageToKafka(testTopicName, "Hello world!!!")
        val consumedRecord = consumeFirstRawRecord[String, String](testTopicName)
        consumedRecord.value() shouldBe "Hello world!!!"

        awaitNumReportedSpans(3)

        assertReportedSpan(_.operationName == "send") { span =>
          span.metricTags.get(plain("component")) shouldBe "kafka.producer"
          span.metricTags.get(plain("span.kind")) shouldBe "producer"
          span.tags.get(plain("kafka.topic")) shouldBe testTopicName
          span.tags.get(plain("kafka.key")) shouldBe Client.Keys.Null
          span.tags.get(plain("kafka.partition")) shouldBe Client.Keys.Null
        }

        assertReportedSpan(_.operationName == "poll") { span =>
          span.metricTags.get(plain("component")) shouldBe "kafka.consumer"
          span.metricTags.get(plain("span.kind")) shouldBe "consumer"
          span.tags.get(plain("kafka.groupId")) should not be empty
          span.tags.get(plain("kafka.clientId")) should not be empty
          span.tags.get(plain("kafka.partitions")) should not be empty
          span.tags.get(plain("kafka.topics")) should not be empty
        }

        assertReportedSpan(_.operationName == "consumed-record") { span =>
          span.metricTags.get(plain("component")) shouldBe "kafka.consumer"
          span.metricTags.get(plain("span.kind")) shouldBe "consumer"
          span.tags.get(plain("kafka.topic")) shouldBe testTopicName
          span.tags.get(plain("kafka.clientId")) should not be empty
          span.tags.get(plain("kafka.groupId")) should not be empty
          span.tags.get(plainLong("kafka.partition")) shouldBe 0L
          span.tags.get(plainLong("kafka.timestamp")) shouldBe consumedRecord.timestamp()
          span.tags.get(plain("kafka.timestampType")) shouldBe consumedRecord.timestampType().name
        }
      }
    }

    "create multiple Producer/Consumer Spans when publish/consume multiple messages - with autoCommit=true (one batch)" in new SpanReportingTestScope(reporter) {

      withRunningKafka {

        publishStringMessageToKafka(testTopicName, "m1")
        publishStringMessageToKafka(testTopicName, "m2")

        implicit val stringDeser = new StringDeserializer

        consumeNumberMessagesFrom[String](testTopicName,2) should contain allOf ("m1", "m2")
        awaitNumReportedSpans(5)
        // one poll operation (batch due to autoCommit=true)
        assertReportedSpans(s => s.operationName == "poll"){ spans =>
          spans should have size 1
        }
        assertReportedSpans(_.operationName == "consumed-record"){ spans =>
          spans should have size 2
          spans.map(_.trace.id.string).distinct should have size 2
        }
        DotFileGenerator.dumpToDotFile("client-autoCommit", reportedSpans)
      }
    }

    "create multiple Producer/Consumer Spans when publish/consume multiple messages - with autoCommit=false (multiple polls)" in new SpanReportingTestScope(reporter) {

      withRunningKafka {

        publishStringMessageToKafka(testTopicName, "m1")
        publishStringMessageToKafka(testTopicName, "m2")

        consumeFirstStringMessageFrom(testTopicName, autoCommit = false) shouldBe "m1"
        consumeFirstStringMessageFrom(testTopicName, autoCommit = false) shouldBe "m2"
        awaitNumReportedSpans(7)
        // two poll operations
        assertReportedSpans(s => s.operationName == "poll"){ spans =>
          spans should have size 2
        }
        assertReportedSpans(_.operationName == "consumed-record"){ spans =>
          spans should have size 3
          spans.map(_.trace.id.string).distinct should have size 2
        }
        DotFileGenerator.dumpToDotFile("client-noAutoCommit", reportedSpans)
      }
    }

    "create a Producer/Consumer Span when publish/consume a message without follow-strategy and expect a linked span" in new SpanReportingTestScope(reporter) {
      withRunningKafka {

        Kamon.reconfigure(ConfigFactory.parseString("kamon.instrumentation.kafka.client.follow-strategy = false").withFallback(Kamon.config()))

        publishStringMessageToKafka(testTopicName, "Hello world!!!")
        consumeFirstStringMessageFrom(testTopicName, autoCommit = true) shouldBe "Hello world!!!"

        awaitNumReportedSpans(3)

        var sendingSpan: Option[Span.Finished] = None
        assertReportedSpan(_.operationName == "send") { span =>
          span.metricTags.get(plain("span.kind")) shouldBe "producer"
          span.metricTags.get(plain("component")) shouldBe "kafka.producer"
          span.tags.get(plain("kafka.clientId")) should not be empty
          span.tags.get(plain("kafka.topic")) shouldBe testTopicName
          span.tags.get(plain("kafka.key")) shouldBe Client.Keys.Null
          span.tags.get(plain("kafka.partition")) shouldBe Client.Keys.Null
          sendingSpan = Some(span)
        }

        assertReportedSpan(_.operationName == "poll") { span =>
          span.metricTags.get(plain("span.kind")) shouldBe "consumer"
          span.metricTags.get(plain("component")) shouldBe "kafka.consumer"
        }

        assertReportedSpan(_.operationName == "consumed-record") { span =>
          span.metricTags.get(plain("span.kind")) shouldBe "consumer"
          span.metricTags.get(plain("component")) shouldBe "kafka.consumer"
          span.tags.get(plain("kafka.topic")) shouldBe testTopicName
          span.tags.get(plain("kafka.clientId")) should not be empty
          span.tags.get(plain("kafka.groupId")) should not be empty
          span.tags.get(plainLong("kafka.partition")) shouldBe 0L
          span.links should have size 2 // poll-span AND the original send span
          val sendinglinks = span.links.filter(_.trace.id == sendingSpan.get.trace.id)
          sendinglinks should have size 1
          sendinglinks.head.trace.id shouldBe sendingSpan.get.trace.id
          sendinglinks.head.spanId shouldBe sendingSpan.get.id
        }

        assertNoSpansReported()
      }
    }

    "create a Producer/Consumer Span when publish/consume a message with delayed spans" in new SpanReportingTestScope(reporter) {
      withRunningKafka {

        Kamon.reconfigure(ConfigFactory.parseString("""
            |kamon.instrumentation.kafka.client.use-delayed-spans = true
            |kamon.instrumentation.kafka.client.follow-strategy = false
        """.stripMargin).withFallback(Kamon.config()))
        Client.useDelayedSpans shouldBe true

        publishStringMessageToKafka(testTopicName, "Hello world!!!")
        consumeFirstStringMessageFrom(testTopicName) shouldBe "Hello world!!!"

        awaitNumReportedSpans(3)

        var sendingSpan: Option[Span.Finished] = None
        assertReportedSpan(_.operationName == "send") { span =>
          span.metricTags.get(plain("span.kind")) shouldBe "producer"
          span.metricTags.get(plain("component")) shouldBe "kafka.producer"
          span.tags.get(plain("kafka.clientId")) should not be empty
          span.tags.get(plain("kafka.topic")) shouldBe testTopicName
          span.tags.get(plain("kafka.key")) shouldBe Client.Keys.Null
          span.tags.get(plain("kafka.partition")) shouldBe Client.Keys.Null
          sendingSpan = Some(span)
        }

        assertReportedSpan(_.operationName == "poll") { span =>
          span.metricTags.get(plain("span.kind")) shouldBe "consumer"
          span.metricTags.get(plain("component")) shouldBe "kafka.consumer"
        }

        assertReportedSpan(_.operationName == "consumed-record") { span =>
          span.wasDelayed shouldBe true
          span.metricTags.get(plain("span.kind")) shouldBe "consumer"
          span.metricTags.get(plain("component")) shouldBe "kafka.consumer"
          span.tags.get(plain("kafka.topic")) shouldBe testTopicName
          span.tags.get(plain("kafka.clientId")) should not be empty
          span.tags.get(plain("kafka.groupId")) should not be empty
          span.tags.get(plainLong("kafka.partition")) shouldBe 0L
          span.links should have size 2
          val sendinglinks = span.links.filter(_.trace.id == sendingSpan.get.trace.id)
          sendinglinks should have size 1
          val link = sendinglinks.head
          link.trace.id shouldBe sendingSpan.get.trace.id
          link.spanId shouldBe sendingSpan.get.id
        }

        assertNoSpansReported()
      }
    }
  }
}
