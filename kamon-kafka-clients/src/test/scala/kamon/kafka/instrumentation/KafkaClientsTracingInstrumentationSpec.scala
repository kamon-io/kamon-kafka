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

import kamon.Kamon
import kamon.testkit.{MetricInspection, Reconfigure, TestSpanReporter}
import kamon.trace.Span.TagValue
import kamon.util.Registration
import net.manub.embeddedkafka.{Consumers, EmbeddedKafka}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar
import org.scalatest.{BeforeAndAfterAll, Matchers, OptionValues, WordSpec}

class KafkaClientsTracingInstrumentationSpec extends WordSpec
  with Matchers
  with Eventually
  with SpanSugar
  with BeforeAndAfterAll
  with EmbeddedKafka
  with MetricInspection
  with Reconfigure
  with OptionValues with Consumers {

  "the KafkaClientsTracingInstrumentation" should {
    "generate a Producer Span when publish a message" in {
      withRunningKafka {

        publishStringMessageToKafka("kamon.topic", "Hello world!!!!!")

        eventually(timeout(10 seconds)) {
          val span = reporter.nextSpan().value
          span.operationName shouldBe "kafka.produce"
          span.tags("span.kind") shouldBe TagValue.String("producer")
          span.tags("kafka.key") shouldBe TagValue.String("unknown-key")
          span.tags("kafka.partition") shouldBe TagValue.String("unknown-partition")
          span.tags("kafka.topic") shouldBe TagValue.String("kamon.topic")
          reporter.nextSpan() shouldBe None
        }
      }
    }

    "generate a Producer/Consumer Span when publish/read a message" in {
      withRunningKafka {

        publishStringMessageToKafka("kamon.topic", "Hello world!!!")
        consumeFirstStringMessageFrom("kamon.topic") shouldBe "Hello world!!!"

        eventually(timeout(10 seconds)) {
          val span = reporter.nextSpan().value
          span.operationName shouldBe "kafka.produce"
          span.tags("span.kind") shouldBe TagValue.String("producer")
          span.tags("kafka.key") shouldBe TagValue.String("unknown-key")
          span.tags("kafka.partition") shouldBe TagValue.String("unknown-partition")
          span.tags("kafka.topic") shouldBe TagValue.String("kamon.topic")
        }

        eventually(timeout(10 seconds)) {
          val span = reporter.nextSpan().value
          span.operationName shouldBe "poll"
          span.tags("span.kind") shouldBe TagValue.String("consumer")
          span.tags("kafka.partition") shouldBe TagValue.Number(0)
          span.tags("kafka.topic") shouldBe TagValue.String("kamon.topic")
          reporter.nextSpan() shouldBe None
        }
      }
    }
  }

  var registration: Registration = _
  val reporter = new TestSpanReporter()

  override protected def beforeAll(): Unit = {
    enableFastSpanFlushing()
    sampleAlways()
    registration = Kamon.addReporter(reporter)
  }

  override protected def afterAll(): Unit = {
    registration.cancel()
  }
}