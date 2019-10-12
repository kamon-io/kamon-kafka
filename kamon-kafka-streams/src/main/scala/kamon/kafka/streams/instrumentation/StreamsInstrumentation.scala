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

package kamon.kafka.streams.instrumentation

import kamon.Kamon
import kamon.context.Context
import kamon.kafka.client.instrumentation.ContextSerializationHelper
import kamon.kafka.streams.Streams
import kamon.trace.Span
import kanela.agent.api.instrumentation.InstrumentationBuilder
import kanela.agent.libs.net.bytebuddy.asm.Advice
import org.apache.kafka.common.header.Header
import org.apache.kafka.streams.processor.internals.{ProcessorNode, StampedRecord, StreamTask}


class StreamsInstrumentation extends InstrumentationBuilder {

  /**
    * Instruments org.apache.kafka.streams.processor.internals.ProcessorNode::process
    *
    * This will start and finish the stream topology node span
    */
  onType("org.apache.kafka.streams.processor.internals.ProcessorNode")
    .advise(method("process"), classOf[ProcessorNodeProcessMethodAdvisor])

  /**
    * Instruments org.apache.kafka.streams.processor.internals.StreamTask::updateProcessorContext
    *
    * UpdateProcessContextAdvisor: this will start the stream span
    * ProcessMethodAdvisor: this will finish the stream span
    *
    */
  onType("org.apache.kafka.streams.processor.internals.StreamTask")
    .advise(method("updateProcessorContext"), classOf[UpdateProcessContextAdvisor])
    .advise(method("process"), classOf[ProcessMethodAdvisor])

}

object ContextHelper {
  def     getContext(h: Header): Context = {
    Option(h) match {
      case Some(x) =>
        ContextSerializationHelper.fromByteArray(x.value());
      case _ =>
        Context.Empty
    }
  }
}

class UpdateProcessContextAdvisor
object UpdateProcessContextAdvisor {

  @Advice.OnMethodExit(onThrowable = classOf[Throwable], suppress = classOf[Throwable])
  def onExit(
              @Advice.Argument(0) record: StampedRecord,
              @Advice.Argument(1) currNode: ProcessorNode[_, _],
              @Advice.This streamTask:StreamTask,
              @Advice.Thrown throwable: Throwable):Unit = {

    val header = record.headers().lastHeader("kamon-context");
    val currentContext = ContextHelper.getContext(header);
    val span = Kamon.consumerSpanBuilder(streamTask.applicationId(), "kafka.stream")
      .asChildOf(currentContext.get(Span.Key))
      .tagMetrics("kafka.topic", record.topic())
      .tag("kafka.partition", record.partition())
      .tag("kafka.offset", record.offset())
      .start

    Kamon.storeContext(Context.of(Span.Key, span));
  }
}

class ProcessorNodeProcessMethodAdvisor
object ProcessorNodeProcessMethodAdvisor {
  @Advice.OnMethodEnter
  def onEnter(@Advice.This node: ProcessorNode[_,_]): Context = {
    if(Streams.traceNodes) {
      val currentSpan = Kamon.currentSpan()
      val span = Kamon.spanBuilder(node.name())
        .asChildOf(currentSpan)
        .tagMetrics("span.kind", "processor")
        .tagMetrics("component", "kafka.stream.node")
        .start()
      Context.of(Span.Key, span)
    } else
      Context.Empty
  }

  @Advice.OnMethodExit(onThrowable = classOf[Throwable], suppress = classOf[Throwable])
  def onExit(@Advice.This node: ProcessorNode[_,_], @Advice.Enter ctx: Context, @Advice.Thrown throwable: Throwable):Unit = {
    if(Streams.traceNodes) {
      val currentSpan = ctx.get(Span.Key)
      if (throwable != null) currentSpan.fail(throwable.getMessage)
      currentSpan.finish()
    }
  }
}

class ProcessMethodAdvisor
object ProcessMethodAdvisor {

  @Advice.OnMethodExit(onThrowable = classOf[Throwable], suppress = classOf[Throwable])
  def onExit(@Advice.Origin r: Any, @Advice.This streamTask:StreamTask, @Advice.Return recordProcessed: Boolean, @Advice.Thrown throwable: Throwable):Unit = {

    val currentSpan = Kamon.currentSpan()
    if(recordProcessed) {
      currentSpan.mark(s"kafka.streams.task.id=${streamTask.id()}")
      currentSpan.tag("kafka.applicationId", streamTask.applicationId())

      if(throwable != null) currentSpan.fail(throwable.getMessage)
      currentSpan.finish()
    }
  }
}

