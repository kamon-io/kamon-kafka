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

package kamon.kafka.streams.instrumentation

import com.typesafe.config.ConfigFactory
import kamon.Kamon
import kamon.kafka.client
import kamon.kafka.client.Client
import kamon.kafka.instrumentation.{SpanReportingTestScope, TestSpanReporting}
import kamon.tag.Lookups._
import kamon.testkit.Reconfigure
import net.manub.embeddedkafka.Codecs._
import net.manub.embeddedkafka.ConsumerExtensions._
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import net.manub.embeddedkafka.streams.EmbeddedKafkaStreamsAllInOne
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.scala
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

    "ensure continuation of traces from 'regular' publishers and streams with 'followStrategy' and assert stream and node spans are poresent" in new SpanReportingTestScope(reporter) with ConfigSupport {

      // Explicitly enable follow-strategy ...
      reconfigureKamon("kamon.kafka.client.follow-strategy = true")
      // ... and ensure that it is active
      Client.followStrategy shouldBe true

      runStreams(Seq(inTopic, outTopic), buildExampleTopology) {
        publishToKafka(inTopic, "hello", "world!")

        withConsumer[String, String, Unit] { consumer =>
          val consumedMessages: Stream[(String, String)] = consumer.consumeLazily(outTopic)
          consumedMessages.take(1) should be(Seq("hello" -> "world!"))
        }

        awaitNumReportedSpans(7)

        reportedSpans.map(_.trace.id.string).distinct should have size 1
        reportedSpans.foreach(s => println(s"Span: op=${s.operationName}, comp=${s.tags.get(plain("component"))}"))
      }
    }

    "Disable node span creation in config and assert on stream span present" in new SpanReportingTestScope(reporter) with ConfigSupport {

      // Explicitly set desired configuration ...
      reconfigureKamon("""
          |kamon.kafka.client.follow-strategy = true
          |kamon.kafka.streams.trace-nodes = false
        """.stripMargin)
      // ... and ensure that it is active
      client.Client.followStrategy shouldBe true
      kamon.kafka.streams.Streams.traceNodes shouldBe false

      runStreams(Seq(inTopic, outTopic), buildExampleTopology) {
        publishToKafka(inTopic, "hello", "world!")

        withConsumer[String, String, Unit] { consumer =>
          val consumedMessages: Stream[(String, String)] = consumer.consumeLazily(outTopic)
          consumedMessages.take(1) should be(Seq("hello" -> "world!"))
        }

        awaitNumReportedSpans(5)

        reportedSpans.map(_.trace.id.string).distinct should have size 1
        reportedSpans.foreach(s => println(s"Span: op=${s.operationName}, comp=${s.tags.get(plain("component"))}"))
      }
    }

    "Multiple messages in a flow - NO node tracing" in new SpanReportingTestScope(reporter) with ConfigSupport {

      // Explicitly enable follow-strategy and DISABLE node tracing ....
      reconfigureKamon("""
          |kamon.kafka.client.follow-strategy = true
          |kamon.kafka.streams.trace-nodes = false
        """.stripMargin)
      // ... and ensure that it is active
      client.Client.followStrategy shouldBe true
      kamon.kafka.streams.Streams.traceNodes shouldBe false

      runStreams(Seq(inTopic, outTopic), buildExampleTopology) {
        publishToKafka(inTopic, "k1", "v1")
        publishToKafka(inTopic, "k2", "v2")
        publishToKafka(inTopic, "k3", "v3")

        withConsumer[String, String, Unit] { consumer =>
          val consumedMessages: Stream[(String, String)] = consumer.consumeLazily(outTopic)
          consumedMessages.take(3) should be(Seq("k1" -> "v1", "k2" -> "v2", "k3" -> "v3"))
        }

        awaitNumReportedSpans(11)

        reportedSpans.map(_.trace.id.string).distinct should have size 3 // for each message initially send
        reportedSpans.foreach(s => println(s"Span: op=${s.operationName}, comp=${s.tags.get(plain("component"))}"))
        DotFileGenerator.dumpToDotFile("stream.dot", reportedSpans)
      }
    }

    "Multiple messages in a flow - WITH node tracing" in new SpanReportingTestScope(reporter) with ConfigSupport {

      // Explicitly enable follow-strategy and node tracing ....
      reconfigureKamon("""
          |kamon.kafka.client.follow-strategy = true
          |kamon.kafka.streams.trace-nodes = true
        """.stripMargin)

      // ... and ensure that it is active
      client.Client.followStrategy shouldBe true
      kamon.kafka.streams.Streams.traceNodes shouldBe true

      runStreams(Seq(inTopic, outTopic), buildExampleTopology) {
        publishToKafka(inTopic, "k1", "v1")
        publishToKafka(inTopic, "k2", "v2")
        publishToKafka(inTopic, "k3", "v3")

        withConsumer[String, String, Unit] { consumer =>
          val consumedMessages: Stream[(String, String)] = consumer.consumeLazily(outTopic)
          consumedMessages.take(3) should be(Seq("k1" -> "v1", "k2" -> "v2", "k3" -> "v3"))
        }

        awaitNumReportedSpans(17)

        reportedSpans.map(_.trace.id.string).distinct should have size 3 // for each message initially send
        reportedSpans.foreach(s => println(s"Span: op=${s.operationName}, comp=${s.tags.get(plain("component"))}"))
        DotFileGenerator.dumpToDotFile("stream.dot", reportedSpans)
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