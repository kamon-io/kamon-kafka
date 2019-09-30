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
import kamon.module.Module.Registration
import kamon.Kamon
import kamon.tag.Lookups._
import kamon.testkit.{Reconfigure, TestSpanReporter}
import kamon.trace.Span
import net.manub.embeddedkafka.Codecs._
import net.manub.embeddedkafka.ConsumerExtensions._
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import net.manub.embeddedkafka.streams.EmbeddedKafkaStreamsAllInOne
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.{StreamsBuilder, scala}
import org.apache.kafka.streams.kstream.{Consumed, KStream, Produced}
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
  with OptionValues {

  import net.manub.embeddedkafka.Codecs.stringKeyValueCrDecoder

  // increase zk connection timeout to avoid failing tests in "slow" environments
  implicit val defaultConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig.apply(
      customBrokerProperties = EmbeddedKafkaConfig(kafkaPort = 7000, zooKeeperPort = 7001).customBrokerProperties + ("zookeeper.connection.timeout.ms" -> "20000"))

  val (inTopic, outTopic) = ("in", "out")

  val stringSerde: Serde[String] = Serdes.String()

  "The Kafka Streams Tracing Instrumentation" should {
    "create a Producer/Stream Span when publish and read from the stream" in {
      val streamBuilder = new StreamsBuilder
      val stream: KStream[String, String] = streamBuilder.stream(inTopic, Consumed.`with`(stringSerde, stringSerde))

      stream.to(outTopic, Produced.`with`(stringSerde, stringSerde))

      runStreams(Seq(inTopic, outTopic), streamBuilder.build()) {
        publishToKafka(inTopic, "hello", "world!")
        publishToKafka(inTopic, "kamon", "rocks!")
        publishToKafka(inTopic, "foo", "bar")

        withConsumer[String, String, Unit] { consumer =>
          val consumedMessages: Stream[(String, String)] =  consumer.consumeLazily(outTopic)
          consumedMessages.take(2) should be(Seq("hello" -> "world!", "kamon" -> "rocks!"))
          consumedMessages.drop(2).head should be("foo" -> "bar")
        }

        eventually(timeout(10 seconds)) {
          val span = reporter.nextSpan().value
          span.operationName shouldBe "publish"
          span.tags.get(plain("component")) shouldBe "kafka.publisher"
          span.tags.get(plain("span.kind")) shouldBe "producer"
          span.tags.get(plain("kafka.key")) shouldBe "hello"
          span.tags.get(plain("kafka.partition")) shouldBe "unknown-partition"
          span.tags.get(plain("kafka.topic")) shouldBe "in"
        }

        eventually(timeout(10 seconds)) {
          val span = reporter.nextSpan().value
//          span.operationName shouldBe "stream"
          span.tags.get(plain("component")) shouldBe "kafka.stream"
          span.tags.get(plain("span.kind")) shouldBe "consumer"
          span.tags.get(plainLong("kafka.partition")) shouldBe 0L
          span.tags.get(plain("kafka.topic")) shouldBe "in"
          span.tags.get(plainLong("kafka.offset")) shouldBe 0L
        }
      }
    }

    "ensure continuation of traces from 'regular' publishers and streams with 'followStrategy' and assert stream and node spans are poresent" in {

      // Explicitly enable follow-strategy ...
      Kamon.reconfigure(ConfigFactory.parseString("kamon.kafka.follow-strategy = true").withFallback(Kamon.config()))
      // ... and ensure that it is active
      kamon.kafka.Kafka.followStrategy shouldBe true

      runStreams(Seq(inTopic, outTopic), buildExampleTopology) {
        publishToKafka(inTopic, "hello", "world!")

        withConsumer[String, String, Unit] { consumer =>
          val consumedMessages: Stream[(String, String)] =  consumer.consumeLazily(outTopic)
          consumedMessages.take(1) should be(Seq("hello" -> "world!"))
        }

        eventually(timeout(5 seconds)) {
          reporter.nextSpan().foreach{ s =>
            reportedSpans = s :: reportedSpans
          }
          reportedSpans should have size 7
          reportedSpans.map(_.trace.id.string).distinct should have size 1
          reportedSpans.foreach(s => println(s"Span: op=${s.operationName}, comp=${s.tags.get(plain("component"))}"))
        }
      }
    }

    "Disable node span creation in config and assert on stream span present" in {

      // Explicitly enable follow-strategy ....
      val config =
        """
          |kamon.kafka.follow-strategy = true
          |kamon.kafka.streams.trace-nodes = false
        """.stripMargin

      Kamon.reconfigure(ConfigFactory.parseString(config).withFallback(Kamon.config()))

      // ... and ensure that it is active
      kamon.kafka.Kafka.followStrategy shouldBe true
      kamon.kafka.streams.Streams.traceNodes shouldBe false

      runStreams(Seq(inTopic, outTopic), buildExampleTopology) {
        publishToKafka(inTopic, "hello", "world!")

        withConsumer[String, String, Unit] { consumer =>
          val consumedMessages: Stream[(String, String)] =  consumer.consumeLazily(outTopic)
          consumedMessages.take(1) should be(Seq("hello" -> "world!"))
        }

        eventually(timeout(5 seconds)) {
          reporter.nextSpan().foreach{ s =>
            reportedSpans = s :: reportedSpans
          }
          reportedSpans should have size 5
          reportedSpans.map(_.trace.id.string).distinct should have size 1
          reportedSpans.foreach(s => println(s"Span: op=${s.operationName}, comp=${s.tags.get(plain("component"))}"))
        }
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

  var reportedSpans: List[Span.Finished] = Nil
  var registration: Registration = _
  val reporter = new TestSpanReporter.BufferingSpanReporter()

  before {
    reportedSpans = Nil
    reporter.clear()
  }

  override protected def beforeAll(): Unit = {
    enableFastSpanFlushing()
    sampleAlways()
    registration = Kamon.registerModule("testReporter", reporter)
  }

  override protected def afterAll(): Unit = {
    registration.cancel()
  }
}