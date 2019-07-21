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

import kamon.module.Module.Registration
import kamon.Kamon
import kamon.tag.Lookups._
import kamon.testkit.{Reconfigure, TestSpanReporter}
import net.manub.embeddedkafka.Codecs._
import net.manub.embeddedkafka.ConsumerExtensions._
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import net.manub.embeddedkafka.streams.EmbeddedKafkaStreamsAllInOne
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.{Consumed, KStream, Produced}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar
import org.scalatest.{BeforeAndAfterAll, Matchers, OptionValues, WordSpec}

class KafkaStreamsTracingInstrumentationSpec extends WordSpec
  with EmbeddedKafkaStreamsAllInOne
  with Matchers
  with Eventually
  with SpanSugar
  with BeforeAndAfterAll
  with Reconfigure
  with OptionValues {

  import net.manub.embeddedkafka.Codecs.stringKeyValueCrDecoder

  implicit val config = EmbeddedKafkaConfig(kafkaPort = 7000, zooKeeperPort = 7001)

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
          span.operationName shouldBe "kafka.produce"
          span.tags.get(plain("span.kind")) shouldBe "producer"
          span.tags.get(plain("kafka.key")) shouldBe "hello"
          span.tags.get(plain("kafka.partition")) shouldBe "unknown-partition"
          span.tags.get(plain("kafka.topic")) shouldBe "in"
        }

        eventually(timeout(10 seconds)) {
          val span = reporter.nextSpan().value
          span.operationName shouldBe "stream"
          span.tags.get(plain("span.kind")) shouldBe "consumer"
          span.tags.get(plainLong("kafka.partition")) shouldBe 0L
          span.tags.get(plain("kafka.topic")) shouldBe "in"
          span.tags.get(plainLong("kafka.offset")) shouldBe 0L
        }
      }
    }
  }

  var registration: Registration = _
  val reporter = new TestSpanReporter.BufferingSpanReporter()

  override protected def beforeAll(): Unit = {
    enableFastSpanFlushing()
    sampleAlways()
    registration = Kamon.registerModule("testReporter", reporter)
  }

  override protected def afterAll(): Unit = {
    registration.cancel()
  }
}