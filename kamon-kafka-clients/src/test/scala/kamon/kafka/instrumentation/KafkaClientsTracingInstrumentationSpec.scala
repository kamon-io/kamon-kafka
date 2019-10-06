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
import kamon.module.Module.Registration
import kamon.tag.Lookups._
import kamon.testkit.{Reconfigure, TestSpanReporter}
import kamon.trace.Span
import net.manub.embeddedkafka.{Consumers, EmbeddedKafka, EmbeddedKafkaConfig}
import org.scalatest.concurrent.{Eventually, PatienceConfiguration}
import org.scalatest.time.SpanSugar
import org.scalatest.{BeforeAndAfterAll, Matchers, OptionValues, WordSpec}

class KafkaClientsTracingInstrumentationSpec extends WordSpec
  with Matchers
  with Eventually
  with SpanSugar
  with BeforeAndAfterAll
  with EmbeddedKafka
  with Reconfigure
  with OptionValues with Consumers {

  // increase zk connection timeout to avoid failing tests in "slow" environments
  implicit val defaultConfig: EmbeddedKafkaConfig =
    EmbeddedKafkaConfig.apply(customBrokerProperties = EmbeddedKafkaConfig.apply().customBrokerProperties + ("zookeeper.connection.timeout.ms" -> "20000"))

  "The Kafka Clients Tracing Instrumentation" should {
    "create a Producer Span when publish a message" in {
      withRunningKafka {

        publishStringMessageToKafka("kamon.topic", "Hello world!!!!!")

        eventually(timeout(10 seconds)) {
          val span = reporter.nextSpan().value
          span.operationName shouldBe "publish"
          span.tags.get(plain("component")) shouldBe "kafka.publisher"
          span.tags.get(plain("span.kind")) shouldBe "producer"
          span.tags.get(plain("kafka.key")) shouldBe "unknown-key"
          span.tags.get(plain("kafka.partition")) shouldBe "unknown-partition"
          span.tags.get(plain("kafka.topic")) shouldBe "kamon.topic"
          reporter.nextSpan() shouldBe None
        }
      }
    }

    "create a Producer/Consumer Span when publish/consume a message" in new TestScope {

      withRunningKafka {

        publishStringMessageToKafka("kamon.topic", "Hello world!!!")
        consumeFirstStringMessageFrom("kamon.topic") shouldBe "Hello world!!!"

        expectNumSpans(2, timeout(20 seconds))

        assertReportedSpan(_.operationName == "publish") { span =>
          span.tags.get(plain("component")) shouldBe "kafka.publisher"
          span.tags.get(plain("span.kind")) shouldBe "producer"
          span.tags.get(plain("kafka.key")) shouldBe "unknown-key"
          span.tags.get(plain("kafka.partition")) shouldBe "unknown-partition"
          span.tags.get(plain("kafka.topic")) shouldBe "kamon.topic"
        }

        assertReportedSpan(_.operationName == "poll") { span =>
          span.operationName shouldBe "poll"
          span.tags.get(plain("span.kind")) shouldBe "consumer"
          span.tags.get(plainLong("kafka.partition")) shouldBe 0L
          span.tags.get(plain("kafka.topic")) shouldBe "kamon.topic"
        }
      }
    }

    "create multiple Producer/Consumer Spans when publish/consume multiple messages" in new TestScope {

      withRunningKafka {

        publishStringMessageToKafka("kamon.topic", "m1")
        publishStringMessageToKafka("kamon.topic", "m2")
        consumeFirstStringMessageFrom("kamon.topic") shouldBe "m1"
        consumeFirstStringMessageFrom("kamon.topic") shouldBe "m2"

        expectNumSpans(4,timeout(10 seconds))
        val traceIds = reportedSpans.map(_.trace.id.string).distinct
        traceIds should have size 2
        assertReportedSpans(_.trace.id.string == traceIds.head){ spans => spans should have size 2}
        assertReportedSpans(_.trace.id.string == traceIds(1)){ spans => spans should have size 2}
      }
    }

    "create a Producer/Consumer Span when publish/consume a message without follow-strategy" in new TestScope {
      withRunningKafka {

        Kamon.reconfigure(ConfigFactory.parseString("kamon.kafka.follow-strategy = false").withFallback(Kamon.config()))

        publishStringMessageToKafka("kamon.topic", "Hello world!!!")
        consumeFirstStringMessageFrom("kamon.topic") shouldBe "Hello world!!!"

        expectNumSpans(2, timeout(10 seconds))

        assertReportedSpan(_.operationName == "publish") { span =>
          span.tags.get(plain("span.kind")) shouldBe "producer"
          span.tags.get(plain("component")) shouldBe "kafka.publisher"
          span.tags.get(plain("kafka.key")) shouldBe "unknown-key"
          span.tags.get(plain("kafka.partition")) shouldBe "unknown-partition"
          span.tags.get(plain("kafka.topic")) shouldBe "kamon.topic"
        }

        assertReportedSpan(_.operationName == "poll") { span =>
          span.tags.get(plain("span.kind")) shouldBe "consumer"
          span.tags.get(plain("component")) shouldBe "kafka.consumer"
          span.tags.get(plainLong("kafka.partition")) shouldBe 0L
          span.tags.get(plain("kafka.topic")) shouldBe "kamon.topic"
          span.tags.get(plain("trace.related.trace_id")) should not be null
          span.tags.get(plain("trace.related.span_id")) should not be null
          reporter.nextSpan() shouldBe None
        }
      }
    }
  }

  abstract class TestScope {
    private var _reportedSpans: List[Span.Finished] = Nil
    def reportedSpans = _reportedSpans

    def expectNumSpans(numOfExpectedSpans: Int, timeout: PatienceConfiguration.Timeout): Unit = {
      eventually(timeout) {
        collectReportedSpans()
        _reportedSpans should have size numOfExpectedSpans
      }
      reporter.nextSpan() shouldBe empty
    }

    def collectReportedSpans() =
      reporter.nextSpan().foreach { s =>
        _reportedSpans = s :: _reportedSpans
      }

    def assertReportedSpan(p: Span.Finished => Boolean)(f: Span.Finished => Unit): Unit = {
      _reportedSpans.filter(p) match {
        case Nil =>
          fail("expected span not found!")
        case x :: Nil =>
          f(x)
        case x :: y =>
          fail(s"More than one span found! spans:${x :: y}")
      }
    }

    def assertReportedSpans(p: Span.Finished => Boolean)(f: List[Span.Finished] => Unit): Unit = {
      _reportedSpans.filter(p) match {
        case Nil =>
          fail("expected span not found!")
        case x =>
          f(x)
      }
    }
  }

  var registration: Registration = _
  val reporter = new TestSpanReporter.BufferingSpanReporter

  override protected def beforeAll(): Unit = {
    enableFastSpanFlushing()
    sampleAlways()
    registration = Kamon.registerModule("TestSpanReporter", reporter)
  }

  override protected def afterAll(): Unit = {
    registration.cancel()
  }
}