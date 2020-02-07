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

import java.time.Duration

import com.typesafe.config.ConfigFactory
import kamon.context.Context
import kamon.Kamon
import kamon.instrumentation.kafka.client.KafkaInstrumentation
import kamon.instrumentation.kafka.streams.testutil.StreamsTestSupport
import kamon.instrumentation.kafka.testutil.{DotFileGenerator, SpanReportingTestScope, TestSpanReporting}
import kamon.tag.Lookups._
import kamon.testkit.Reconfigure
import net.manub.embeddedkafka.ConsumerExtensions._
import net.manub.embeddedkafka.EmbeddedKafkaConfig
import net.manub.embeddedkafka.streams.EmbeddedKafkaStreamsAllInOne
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.kstream.JoinWindows
import org.apache.kafka.streams.processor.{Processor, ProcessorContext, PunctuationType, Punctuator}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology, scala}
import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar
import org.scalatest.{BeforeAndAfter, Matchers, OptionValues, WordSpec}


class KafkaStreamsTracingInstrumentationSpec extends WordSpec
  with EmbeddedKafkaStreamsAllInOne
  with Matchers
  with Eventually
  with SpanSugar
  with BeforeAndAfter
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
      import net.manub.embeddedkafka.Codecs._

      // Explicitly enable follow-strategy ...
      reconfigureKamon(
        """
          |kamon.instrumentation.kafka.client.follow-strategy = true
          |kamon.instrumentation.kafka.streams.trace-nodes = true
          |""".stripMargin)
      // ... and ensure that it is active
      KafkaInstrumentation.followStrategy shouldBe true
      Streams.traceNodes shouldBe true

      val streamAppId = "SimpleStream_AppId"
      runStreams(Seq(inTopic, outTopic), SimpleStream.buildTopology(), extraConfig = Map(StreamsConfig.APPLICATION_ID_CONFIG -> streamAppId)) {
        publishToKafka(inTopic, "1", "prvi")
        publishToKafka(inTopic, "1", "drugi")
        publishToKafka(inTopic, "1", "treci")


        //TODO what
        withConsumer[String, String, Unit] { consumer =>
          val consumedMessages: Stream[(String, String)] = consumer.consumeLazily("intermediate")
          consumedMessages.take(10)
        }

        awaitReportedSpans()
        DotFileGenerator.dumpToDotFile("stream-simple", reportedSpans)

        val streamTraceIds = reportedSpans.filter(_.tags.get(plain("kafka.applicationId")) == streamAppId).map(_.trace.id.string).distinct
        streamTraceIds should have size 1

      }
    }

    new Processor[String] {

      override def init(context: ProcessorContext): Unit = {
        context.schedule(null, PunctuationType.WALL_CLOCK_TIME, new Punctuator {
          override def punctuate(timestamp: Long): Unit = {
            context.headers()
          }
        })
      }

      override def process(key: String, value: V): Unit = {

      }

      override def close(): Unit = ???
    }

/*
    "Disable node span creation in config and assert one stream span present" in new SpanReportingTestScope(reporter) with ConfigSupport {

      import net.manub.embeddedkafka.Codecs._

      // Explicitly set desired configuration ...
      reconfigureKamon("""
          |kamon.instrumentation.kafka.client.follow-strategy = true
          |kamon.instrumentation.kafka.streams.trace-nodes = false
        """.stripMargin)
      // ... and ensure that it is active
      Client.followStrategy shouldBe true
      Streams.traceNodes shouldBe false

      val streamAppId = "SimpleStream_AppId"
      runStreams(Seq(inTopic, outTopic), SimpleStream.buildTopology(), extraConfig = Map(StreamsConfig.APPLICATION_ID_CONFIG -> streamAppId)) {
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

      import net.manub.embeddedkafka.Codecs._

      // Explicitly enable follow-strategy and DISABLE node tracing ....
      reconfigureKamon("""
          |kamon.instrumentation.kafka.client.follow-strategy = true
          |kamon.instrumentation.kafka.streams.trace-nodes = false
        """.stripMargin)
      // ... and ensure that it is active
      Client.followStrategy shouldBe true
      Streams.traceNodes shouldBe false

      val streamAppId = "SimpleStream_AppId"
      runStreams(Seq(inTopic, outTopic), SimpleStream.buildTopology(), extraConfig = Map(StreamsConfig.APPLICATION_ID_CONFIG -> streamAppId)) {
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

    "multiple streams - no nodes" in new SpanReportingTestScope(reporter) with ConfigSupport with StreamsTestSupport {

      import net.manub.embeddedkafka.Codecs._

      disableNodeTracing()

      withRunningKafka {
        Seq(inTopic, outTopic).foreach(topic => createCustomTopic(topic))

        val stream1AppId = "SimpleStream_AppId_1"
        val stream2AppId = "SimpleStream_AppId_2"

        val streams1 = new KafkaStreams(
          SimpleStream.buildTopology(),
          map2Properties(streamsConfig.config(stream1AppId, Map.empty)))
        streams1.start()

        val streams2 = new KafkaStreams(
          SimpleStream.buildTopology(),
          map2Properties(streamsConfig.config(stream2AppId, Map.empty)))
        streams2.start()

        try {

          publishToKafka(inTopic, "k1", "v1")
          publishToKafka(inTopic, "k2", "v2")
          publishToKafka(inTopic, "k3", "v3")

          withConsumer[String, String, Unit] { consumer =>
            val consumedMessages: Stream[(String, String)] = consumer.consumeLazily(outTopic)
            consumedMessages.take(10) should have size 6
          }

          awaitReportedSpans()
          assertNumSpansForOperation(stream1AppId, 3)
          assertNumSpansForOperation(stream2AppId, 3)
          assertNumSpansForOperation("consumed-record", 12)

          DotFileGenerator.dumpToDotFile("stream-multiple-streams-no-nodes", reportedSpans)

        } finally {
          streams1.close()
          streams2.close()
        }
      }
    }

    "multiple streams - sequential" in new SpanReportingTestScope(reporter) with ConfigSupport with StreamsTestSupport {

      import net.manub.embeddedkafka.Codecs._

      val inTopic1 = inTopic + "1"
      val outTopic1 = outTopic + "1"
      val outTopic2 = outTopic + "2"
      disableNodeTracing()

      withRunningKafka {
        Seq(inTopic1, outTopic1, outTopic2).foreach(topic => createCustomTopic(topic))

        val stream1AppId = "SimpleStream_AppId_1"
        val stream2AppId = "SimpleStream_AppId_2"

        val streams1 = new KafkaStreams(
          SimpleStream.buildTopology(inTopic1, outTopic1),
          map2Properties(streamsConfig.config(stream1AppId, Map.empty)))
        streams1.start()

        val streams2 = new KafkaStreams(
          SimpleStream.buildTopology(outTopic1, outTopic2),
          map2Properties(streamsConfig.config(stream2AppId, Map.empty)))
        streams2.start()

        try {

          publishToKafka(inTopic1, "k1", "v1")

          withConsumer[String, String, Unit] { consumer =>
            val consumedMessages: Stream[(String, String)] = consumer.consumeLazily(outTopic2)
            consumedMessages.take(10) should have size 1
          }

          awaitReportedSpans()

          DotFileGenerator.dumpToDotFile("stream-multiple-streams-sequential", reportedSpans)

        } finally {
          streams1.close()
          streams2.close()
        }
      }
    }

    "multiple streams - filtered" in new SpanReportingTestScope(reporter) with ConfigSupport with StreamsTestSupport {

      val notIncludedStreamId = "SimpleStream_AppId_1"
      val includeStreamId = "SimpleStream_AppId_2"

      import net.manub.embeddedkafka.Codecs._

      // Explicitly set includes ....
      reconfigureKamon(s"""
        |kamon.instrumentation.kafka.streams.trace.includes = [ "$includeStreamId"]
        """.stripMargin)

      disableNodeTracing()

      withRunningKafka {
        Seq(inTopic, outTopic).foreach(topic => createCustomTopic(topic))


        val notIncludedStream = new KafkaStreams(
          SimpleStream.buildTopology(assertContextNotEmpty = false),
          map2Properties(streamsConfig.config(notIncludedStreamId, Map.empty)))
        notIncludedStream.start()

        val includedStream = new KafkaStreams(
          SimpleStream.buildTopology(),
          map2Properties(streamsConfig.config(includeStreamId, Map.empty)))
        includedStream.start()

        try {

          publishToKafka(inTopic, "k1", "v1")
          publishToKafka(inTopic, "k2", "v2")
          publishToKafka(inTopic, "k3", "v3")

          withConsumer[String, String, Unit] { consumer =>
            val consumedMessages: Stream[(String, String)] = consumer.consumeLazily(outTopic)
            consumedMessages.take(10) should have size 6
          }

          awaitReportedSpans()
          assertNumSpansForOperation(notIncludedStreamId, 0)
          assertNumSpansForOperation(includeStreamId, 3)
          assertNumSpansForOperation("consumed-record", 12)

          DotFileGenerator.dumpToDotFile("stream-multiple-streams-no-nodes", reportedSpans)

        } finally {
          notIncludedStream.close()
          includedStream.close()
        }
      }
    }

    "multiple partitions and streams threads" in new SpanReportingTestScope(reporter) with ConfigSupport with StreamsTestSupport {

      disableNodeTracing()

      implicit val defaultConfig: EmbeddedKafkaConfig =
        EmbeddedKafkaConfig.apply(
          customProducerProperties  = EmbeddedKafkaConfig.apply().customProducerProperties ++ Map(ProducerConfig.BATCH_SIZE_CONFIG -> "0"),
          customConsumerProperties =  EmbeddedKafkaConfig.apply().customConsumerProperties ++ Map(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true"))

      withRunningKafka {
        Seq(inTopic, outTopic).foreach(topic => createCustomTopic(topic, partitions = 8))

        val stream1AppId = "SimpleStream_AppId_1"

        val streams = new KafkaStreams(
          SimpleStream.buildTopology(),
          map2Properties(streamsConfig.config(stream1AppId,
            Map(
              StreamsConfig.NUM_STREAM_THREADS_CONFIG -> "3",
              ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG -> "500"
            )
          )))
        streams.start()

        try {
          import net.manub.embeddedkafka.Codecs._
          publishToKafka(inTopic,(1 to 10).map(i =>  s"k$i" -> s"v$i"))

          // This is needed in order to the streams a chance to run and produce output
          Thread.sleep(2000)

          withConsumer[String, String, Unit] { consumer =>
            val consumedMessages: Stream[(String, String)] = consumer.consumeLazily(outTopic)
            consumedMessages.take(10) should have size 10
          }

          awaitReportedSpans()
          assertNumSpansForOperation(stream1AppId, 10)
          assertNumSpansForOperation("consumed-record", 20)

          DotFileGenerator.dumpToDotFile("stream-multiple-threads", reportedSpans)

        } finally {
          streams.close()
        }
      }
    }

    "report exceptions as error tags on the span" in new SpanReportingTestScope(reporter) with ConfigSupport with StreamsTestSupport  {
      import net.manub.embeddedkafka.Codecs._

      // Explicitly enable node tracing ...
      reconfigureKamon(
        """
          |kamon.instrumentation.kafka.streams.trace-nodes = true
          |""".stripMargin)
      // ... and ensure that it is active
      Streams.traceNodes shouldBe true

      val streamAppId = "SimpleStream_AppId"
      runStreams(Seq(inTopic, outTopic), SimpleStream.buildTopology(raiseException = true), extraConfig = Map(StreamsConfig.APPLICATION_ID_CONFIG -> streamAppId)) {
        publishToKafka(inTopic, "hello1", "world1!")

        awaitReportedSpans(2000)
        DotFileGenerator.dumpToDotFile("stream-exception-tagged", reportedSpans)
        val streamTraceIds = reportedSpans.filter(_.tags.get(plain("kafka.applicationId")) == streamAppId).map(_.trace.id.string).distinct
        streamTraceIds should have size 1

        // stream span should have marked with error
        val streamSpans = reportedSpans.filter(s => s.operationName == streamAppId && s.trace.id.string == streamTraceIds.head)
        streamSpans should have size 1
        streamSpans.head.hasError shouldBe true
        streamSpans.head.tags.get(any("error.stacktrace")).asInstanceOf[AnyRef] should not be null

        // node span should be marked with error
        val nodeSpans = reportedSpans.filter(s => s.operationName == SimpleStream.PeekNode && s.trace.id.string == streamTraceIds.head)
        nodeSpans should have size 1
        nodeSpans.head.hasError shouldBe true
        nodeSpans.head.tags.get(any("error.stacktrace")).asInstanceOf[AnyRef] should not be null


      }

    }

    "link joined messages in span" in new SpanReportingTestScope(reporter) with ConfigSupport with StreamsTestSupport  {
      import net.manub.embeddedkafka.Codecs._

      // Explicitly enable node tracing ...
      reconfigureKamon(
        """
          |kamon.instrumentation.kafka.streams.trace-nodes = true
          |""".stripMargin)
      // ... and ensure that it is active
      Streams.traceNodes shouldBe true

      val joinTopic = "joinTopic"
      val streamAppId = "SimpleStream_AppId"
      private val joinTopology: Topology = JoinStream.buildTopology(inTopic, joinTopic, outTopic)
      println(joinTopology.describe().toString)
      runStreams(Seq(inTopic, joinTopic, outTopic), joinTopology, extraConfig = Map(StreamsConfig.APPLICATION_ID_CONFIG -> streamAppId)) {
        publishToKafka(joinTopic, "hello", "world")
        publishToKafka(inTopic, "hello", "hello ")

        withConsumer[String, String, Unit] { consumer =>
          val consumedMessages: Stream[(String, String)] = consumer.consumeLazily(outTopic)
          val msgs = consumedMessages.take(10)
          msgs should have size 1
          msgs.head._2 shouldBe "hello world"
        }
        awaitReportedSpans()

        val joinThisStreamTraceIds = reportedSpans.filter(_.operationName == JoinStream.JoinThisNode).map(_.trace.id.string)
        joinThisStreamTraceIds should have size 1
        val joinOtherStreamTraceIds = reportedSpans.filter(_.operationName == JoinStream.JoinOtherNode).map(_.trace.id.string)
        joinOtherStreamTraceIds should have size 1

        assertReportedSpan(s => s.operationName == JoinStream.WindowedNode1 && s.trace.id.string == joinThisStreamTraceIds.head) { windowThis =>
          assertReportedSpan(s => s.operationName == "send" && s.parentId == windowThis.id) { joinThis =>
            joinThis.metricTags.get(plain("kafka.topic")) shouldBe s"$streamAppId-${JoinStream.JoinThisNode}-store-changelog"
          }
        }

        assertReportedSpan(s => s.operationName == JoinStream.WindowedNode2 && s.trace.id.string == joinOtherStreamTraceIds.head) { windowOther =>
          assertReportedSpan(s => s.operationName == "send" && s.parentId == windowOther.id) { joinOther =>
            joinOther.metricTags.get(plain("kafka.topic")) shouldBe s"$streamAppId-${JoinStream.JoinOtherNode}-store-changelog"
          }
        }

        // TODO: assert link between JOIN-THIS and JOIN-OTHER traces

        DotFileGenerator.dumpToDotFile("stream-join", reportedSpans)
      }

    }
*/

  }

  object SimpleStream {

    val SourceNode = "KSTREAM-SOURCE-0000000000"
    val FilterNode = "KSTREAM-FILTER-0000000001"
    val PeekNode = "KSTREAM-PEEK-0000000002"
    val MapValuesNode = "KSTREAM-MAPVALUES-0000000003"
    val SinkNode = "KSTREAM-SINK-0000000004"

    def buildTopology(in: String = inTopic, out: String = outTopic, assertContextNotEmpty: Boolean = true, raiseException: Boolean = false) = {
      import scala.ImplicitConversions._
      import org.apache.kafka.streams.scala.Serdes.String

      val streamBuilder = new scala.StreamsBuilder
      streamBuilder.stream[String, String](in)
        .filter{(k, v) => true }
     /*   .peek { (k, v) =>
          if (assertContextNotEmpty && Kamon.currentContext() == Context.Empty) fail("Kamon.currentContext() == Context.Empty !!")
          if (raiseException) throw new RuntimeException("Peng!")
        }*/
        .mapValues{(k, v) => v}
        // todo: add detailed aggregate logging, including traceId of the previous update to the aggregate object
        //        .groupByKey
        //        .aggregate(""){ (k,v, agg) =>agg + v}
        //        .toStream
        .groupBy((k,v) => (k.toInt % 3).toString)
        .aggregate("")((k, v, va) => v + s" $va")
        .toStream.to(out)

      streamBuilder.build
    }
  }

  object JoinStream {

    val SourceNode = "KSTREAM-SOURCE-0000000000"
    val WindowedNode1 = "KSTREAM-WINDOWED-0000000002"
    val WindowedNode2 = "KSTREAM-WINDOWED-0000000003"
    val JoinThisNode = "KSTREAM-JOINTHIS-0000000004"
    val JoinOtherNode = "KSTREAM-JOINOTHER-0000000005"
    val SinkNode = "KSTREAM-SINK-0000000007"

    def buildTopology(in: String, inToJoin: String, out: String = outTopic) = {
      import scala.ImplicitConversions._
      import org.apache.kafka.streams.scala.Serdes.String

      val streamBuilder = new scala.StreamsBuilder
      val toJoin = streamBuilder.stream[String, String](inToJoin)

      streamBuilder.stream[String, String](in)
        .join(toJoin)({ (v1, v2) =>
          v1 + v2
        }, JoinWindows.of(Duration.ofMillis(1000)))
          .to(out)

      streamBuilder.build
    }
  }
}

trait ConfigSupport { _: Matchers =>

  def reconfigureKamon(config: String): Unit =
    Kamon.reconfigure(ConfigFactory.parseString(config).withFallback(Kamon.config()))

  protected def disableNodeTracing(): Unit = {
    reconfigureKamon(s"""kamon.instrumentation.kafka.streams.trace-nodes = false""")
    // ... and ensure that it is active
    Streams.traceNodes shouldBe false
  }


}