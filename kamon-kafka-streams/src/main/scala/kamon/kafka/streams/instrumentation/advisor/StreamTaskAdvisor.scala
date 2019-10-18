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
package kamon.kafka.streams.instrumentation.advisor

import kamon.Kamon
import kamon.context.Context
import kamon.instrumentation.context.HasContext
import kamon.trace.Span
import kanela.agent.libs.net.bytebuddy.asm.Advice
import org.apache.kafka.streams.processor.internals.{InternalProcessorContext, ProcessorNode, StampedRecord, StreamTask}


class StreamTaskUpdateProcessContextAdvisor
object StreamTaskUpdateProcessContextAdvisor {

  import kamon.kafka.client.instrumentation.SpanExtractionSupport._

  @Advice.OnMethodExit(onThrowable = classOf[Throwable], suppress = classOf[Throwable])
  def onExit(
              @Advice.Argument(0) record: StampedRecord with HasConsumerRecord,
              @Advice.Argument(1) currNode: ProcessorNode[_, _],
              @Advice.This streamTask: StreamTask,
              @Advice.FieldValue("processorContext") processorCtx: InternalProcessorContext with HasContext,
              @Advice.Thrown throwable: Throwable): Unit = {

    val maybeParentSpan = record.consumerRecord.flatMap(_.maybeSpan)
    val spanBuilder = Kamon.consumerSpanBuilder(streamTask.applicationId(), "kafka.stream")
      .tagMetrics("kafka.topic", record.topic())
      .tag("kafka.partition", record.partition())
      .tag("kafka.offset", record.offset())
    maybeParentSpan.foreach(parentSpan => spanBuilder.asChildOf(parentSpan))

    val span = spanBuilder.start()
    processorCtx.setContext(Context.of(Span.Key, span))
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

    val currentSpan = processorCtx.context.get(Span.Key)
    if (recordProcessed) {
      currentSpan.mark(s"kafka.streams.task.id=${streamTask.id()}")
      currentSpan.tag("kafka.applicationId", streamTask.applicationId())

      if (throwable != null) currentSpan.fail(throwable.getMessage)
      currentSpan.finish()
    }
  }
}
