/*
 * =========================================================================================
 * Copyright © 2013-2019 the kamon project <http://kamon.io/>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
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
package kamon.instrumentation.kafka.streams.advisor

import kamon.Kamon
import kamon.context.Context
import kamon.instrumentation.context.HasContext
import kamon.instrumentation.kafka.streams.Streams
import kamon.trace.Span
import kanela.agent.libs.net.bytebuddy.asm.Advice
import org.apache.kafka.streams.processor.internals.{InternalProcessorContext, ProcessorNode, StampedRecord, StreamTask}


//TODO all of tags used here can be extracted from processor context
class StreamTaskUpdateProcessContextAdvisor
object StreamTaskUpdateProcessContextAdvisor {

  import kamon.instrumentation.kafka.client.KafkaInstrumentation._

  @Advice.OnMethodExit(onThrowable = classOf[Throwable], suppress = classOf[Throwable])
  def onExit(
              @Advice.Argument(0) record: StampedRecord with HasConsumerRecord,
              @Advice.Argument(1) currNode: ProcessorNode[_, _],
              @Advice.This streamTask: StreamTask,
              @Advice.FieldValue("processorContext") processorCtx: InternalProcessorContext with HasContext,
              @Advice.Thrown throwable: Throwable): Unit = {

    val applicationId = streamTask.applicationId()

    if(Kamon.filter(Streams.StreamsTraceFilterName).accept(applicationId)) {
      val maybeParentSpan = record.consumerRecord.map(_.context.get(Span.Key))
      val spanBuilder = Kamon.consumerSpanBuilder(applicationId, "kafka.stream")
        .tagMetrics("kafka.source.topic", record.topic())
        .tag("kafka.source.partition", record.partition())
        .tag("kafka.source.offset", record.offset())
      maybeParentSpan.foreach(parentSpan => spanBuilder.asChildOf(parentSpan))
      println("POCEO TASK")
      val span = spanBuilder.start()
      processorCtx.setContext(Context.of(Span.Key, span)) //TODO context propagation broken
    }
  }
}

class StreamTaskProcessMethodAdvisor
object StreamTaskProcessMethodAdvisor {

  @Advice.OnMethodExit(onThrowable = classOf[Throwable], suppress = classOf[Throwable])
  def onExit(@Advice.Origin r: Any,
             @Advice.This streamTask: StreamTask,
             @Advice.Return recordProcessed: Boolean,
             @Advice.FieldValue("processorContext") processorCtx: InternalProcessorContext with HasContext,
             @Advice.Thrown throwable: Throwable): Unit = {

    if(Kamon.filter(Streams.StreamsTraceFilterName).accept(streamTask.applicationId())) {
      val currentSpan = processorCtx.context.get(Span.Key)
      val maybeThrowable = Option(throwable)
      maybeThrowable.foreach(t => currentSpan.fail(t))
      if (recordProcessed || maybeThrowable.nonEmpty) {
        currentSpan.mark(s"kafka.streams.task.id=${streamTask.id()}")
        currentSpan.tag("kafka.applicationId", streamTask.applicationId())
        println("ZAVRSION TASK")
        currentSpan.finish()
      }
    }
  }
}
