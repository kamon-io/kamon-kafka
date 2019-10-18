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

package kamon.instrumentation.kafka.streams

import com.typesafe.config.ConfigFactory
import kamon.Kamon
import kamon.instrumentation.kafka
import kamon.instrumentation.kafka.client.Client
import kamon.instrumentation.kafka.testutil.{DotFileGenerator, SpanReportingTestScope, TestSpanReporting}
import kamon.tag.Lookups._
import kamon.testkit.Reconfigure
import net.manub.embeddedkafka.Codecs._
import net.manub.embeddedkafka.ConsumerExtensions._
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import net.manub.embeddedkafka.streams.EmbeddedKafkaStreamsAllInOne
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.{StreamsConfig, scala}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, Matchers, OptionValues, WordSpec}

class KafkaStreamsTracingInstrumentationSpec extends WordSpec
  with EmbeddedKafkaStreamsAllInOne
  with Matchers
  with Eventually
  with SpanSugar
  with BeforeAndAfter
  with BeforeAndAfterAll
  with Reconfigure
  with OptionValues
  with TestSpanReporting {

  import net.manub.embeddedkafka.Codecs.stringKeyValueCrDecoder

  // increase zk connection timeout to avoid failing tests in "slow" environments
  implicit val defaultConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig.apply(
      customBrokerProperties = EmbeddedKafkaConfig(kafkaPort = 7000, zooKeeperPort = 7001).customBrokerProperties + ("zookeeper.connection.timeout.ms" -> "20000"))

  implicit val patienceConfigTimeout = timeout(20 seconds)

  val (inTopic, outTopic) = ("in", "out")

  val stringSerde: Serde[String] = Serdes.String()

  "The Kafka Streams Tracing Instrumentation" should {

    "ensure continuation of traces from 'regular' publishers and streams with 'followStrategy' and assert stream and node spans are present" in new SpanReportingTestScope(reporter) with ConfigSupport {

      // Explicitly enable follow-strategy ...
      reconfigureKamon("kamon.instrumentation.kafka.client.follow-strategy = true")
      // ... and ensure that it is active
      Client.followStrategy shouldBe true

      val streamAppId = "SimpleStream_AppId"
      runStreams(Seq(inTopic, outTopic), buildExampleTopology, extraConfig = Map(StreamsConfig.APPLICATION_ID_CONFIG -> streamAppId)) {
        publishToKafka(inTopic, "hello", "world!")

        withConsumer[String, String, Unit] { consumer =>
          val consumedMessages: Stream[(String, String)] = consumer.consumeLazily(outTopic)
          consumedMessages.take(1) should be(Seq("hello" -> "world!"))
        }

        awaitNumReportedSpans(9)
        var streamTraceId =
          assertReportedSpan(_.tags.get(plain("kafka.applicationId")) == streamAppId){ span => span.trace.id.string }

        val traceOps = reportedSpans.filter(_.trace.id.string == streamTraceId).map(_.operationName)
        traceOps should have size 7
        traceOps should contain allOf("consumed-record", streamAppId, "KSTREAM-FILTER-0000000001", "KSTREAM-MAPVALUES-0000000002")

        DotFileGenerator.dumpToDotFile("stream-simple", reportedSpans)
      }
    }

    "Disable node span creation in config and assert one stream span present" in new SpanReportingTestScope(reporter) with ConfigSupport {

      // Explicitly set desired configuration ...
      reconfigureKamon("""
          |kamon.instrumentation.kafka.client.follow-strategy = true
          |kamon.instrumentation.kafka.streams.trace-nodes = false
        """.stripMargin)
      // ... and ensure that it is active
      Client.followStrategy shouldBe true
      Streams.traceNodes shouldBe false

      val streamAppId = "SimpleStream_AppId"
      runStreams(Seq(inTopic, outTopic), buildExampleTopology, extraConfig = Map(StreamsConfig.APPLICATION_ID_CONFIG -> streamAppId)) {
        publishToKafka(inTopic, "hello", "world!")

        withConsumer[String, String, Unit] { consumer =>
          val consumedMessages: Stream[(String, String)] = consumer.consumeLazily(outTopic)
          consumedMessages.take(1) should be(Seq("hello" -> "world!"))
        }

        awaitNumReportedSpans(7)

        var streamTraceId =
          assertReportedSpan(_.tags.get(plain("kafka.applicationId")) == streamAppId){ span => span.trace.id.string }

        val traceOps = reportedSpans.filter(_.trace.id.string == streamTraceId).map(_.operationName)
        traceOps should have size 5
        traceOps should contain allOf("consumed-record", streamAppId)

        DotFileGenerator.dumpToDotFile("stream-no-nodes", reportedSpans)
      }
    }

    "Multiple messages in a flow - NO node tracing" in new SpanReportingTestScope(reporter) with ConfigSupport {

      // Explicitly enable follow-strategy and DISABLE node tracing ....
      reconfigureKamon("""
          |kamon.instrumentation.kafka.client.follow-strategy = true
          |kamon.instrumentation.kafka.streams.trace-nodes = false
        """.stripMargin)
      // ... and ensure that it is active
      Client.followStrategy shouldBe true
      Streams.traceNodes shouldBe false

      val streamAppId = "SimpleStream_AppId"
      runStreams(Seq(inTopic, outTopic), buildExampleTopology, extraConfig = Map(StreamsConfig.APPLICATION_ID_CONFIG -> streamAppId)) {
        publishToKafka(inTopic, "k1", "v1")
        publishToKafka(inTopic, "k2", "v2")
        publishToKafka(inTopic, "k3", "v3")

        withConsumer[String, String, Unit] { consumer =>
          val consumedMessages: Stream[(String, String)] = consumer.consumeLazily(outTopic)
          consumedMessages.take(3) should be(Seq("k1" -> "v1", "k2" -> "v2", "k3" -> "v3"))
        }

        awaitNumReportedSpans(17)

        // expect 3 different traceIds - one for each event pushed
        reportedSpans.filter(_.tags.get(plain("kafka.applicationId")) == streamAppId).map(_.trace.id.string) should have size 3

        DotFileGenerator.dumpToDotFile("stream-3-events", reportedSpans)
      }
    }

    "Multiple messages in a flow - WITH node tracing" in new SpanReportingTestScope(reporter) with ConfigSupport {

      // Explicitly enable follow-strategy and node tracing ....
      reconfigureKamon("""
          |kamon.instrumentation.kafka.client.follow-strategy = true
          |kamon.instrumentation.kafka.streams.trace-nodes = true
        """.stripMargin)

      // ... and ensure that it is active
      Client.followStrategy shouldBe true
      Streams.traceNodes shouldBe true

      val streamAppId = "SimpleStream_AppId"
      runStreams(Seq(inTopic, outTopic), buildExampleTopology, extraConfig = Map(StreamsConfig.APPLICATION_ID_CONFIG -> streamAppId)) {
        publishToKafka(inTopic, "k1", "v1")
        publishToKafka(inTopic, "k2", "v2")
        publishToKafka(inTopic, "k3", "v3")

        withConsumer[String, String, Unit] { consumer =>
          val consumedMessages: Stream[(String, String)] = consumer.consumeLazily(outTopic)
          consumedMessages.take(3) should be(Seq("k1" -> "v1", "k2" -> "v2", "k3" -> "v3"))
        }

        awaitNumReportedSpans(23)

        // expect 3 different traceIds - one for each event pushed
        val traceIds = reportedSpans.filter(_.tags.get(plain("kafka.applicationId")) == streamAppId).map(_.trace.id.string)
        traceIds should have size 3
        traceIds.foreach(tId => reportedSpans.filter(_.trace.id.string == tId) should have size 7)

        DotFileGenerator.dumpToDotFile("stream-3-events-with-nodes", reportedSpans)
      }
    }

  }

  private def buildExampleTopology = {
    import scala.ImplicitConversions._
    import org.apache.kafka.streams.scala.Serdes.String

    val streamBuilder = new scala.StreamsBuilder
    streamBuilder.stream[String, String](inTopic)
      .filter((k, v) => true)
      .mapValues((k, v) => v)
      // todo: add detailed aggregate logging, including traceId of the previous update to the aggregate object
      //        .groupByKey
      //        .aggregate(""){ (k,v, agg) =>agg + v}
      //        .toStream
      .to(outTopic)

    streamBuilder.build
  }

}

trait ConfigSupport {
  def reconfigureKamon(config: String): Unit =
    Kamon.reconfigure(ConfigFactory.parseString(config).withFallback(Kamon.config()))
}