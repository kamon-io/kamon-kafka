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

import java.nio.ByteBuffer

import kamon.Kamon
import kamon.context.Context
import kamon.kafka.ContextSpanBinaryEncoder
import kamon.trace.Span
import kanela.agent.api.instrumentation.InstrumentationBuilder
import kanela.agent.libs.net.bytebuddy.asm.Advice
import org.apache.kafka.streams.processor.internals.PartitionGroup.RecordInfo
import org.apache.kafka.streams.processor.internals.{ProcessorNode, StampedRecord, StreamTask}


class StreamsInstrumentation extends InstrumentationBuilder {
  /**
    * Instruments org.apache.kafka.streams.processor.internals.StreamTask::process
    */
  onType("org.apache.kafka.streams.processor.internals.StreamTask")
    .advise(method("process"), classOf[ProcessMethodAdvisor])

  /**
    * Instruments org.apache.kafka.streams.processor.internals.ProcessorNode::process
    */
  onType("org.apache.kafka.streams.processor.internals.ProcessorNode")
    .advise(method("process"), classOf[ProcessorNodeProcessMethodAdvisor])

  /**
    * Instruments org.apache.kafka.streams.processor.internals.PartitionGroup::nextRecord
    */
  onType("org.apache.kafka.streams.processor.internals.PartitionGroup")
    .advise(method("nextRecord").and(withReturnTypes(classOf[org.apache.kafka.streams.processor.internals.StampedRecord])), classOf[NextRecordMethodAdvisor])
}


class ProcessorNodeProcessMethodAdvisor
object ProcessorNodeProcessMethodAdvisor {
  @Advice.OnMethodEnter
  def onEnter(@Advice.This node: ProcessorNode[_,_]): Context = {
    println(s"==> ProcessorNode.onEnter node=${node.name()} / ${node.hashCode()}")
    val currentContext = Kamon.currentContext()
    val span = Kamon.spanBuilder("node")
      .asChildOf(currentContext.get(Span.Key))
      .tag("span.kind", "processor")
      .tag("kafka.stream.node", node.name())
      .start()
    Kamon.store(Context.of(Span.Key, span))
    currentContext
  }

  @Advice.OnMethodExit(onThrowable = classOf[Throwable], suppress = classOf[Throwable])
  def onExit(@Advice.This node: ProcessorNode[_,_], @Advice.Enter ctx: Context, @Advice.Thrown throwable: Throwable):Unit = {
    println(s"==> ProcessorNode.onExit node=${node.name()} / ${node.hashCode()}")
    val currentSpan = Kamon.currentSpan
    if(throwable != null) currentSpan.fail(throwable.getMessage)
    currentSpan.finish()
    Kamon.store(ctx)
  }
}

class ProcessMethodAdvisor
object ProcessMethodAdvisor {
  @Advice.OnMethodEnter
  def onEnter(@Advice.This streamTask:StreamTask): Context = {
    println(s"==> StreamTask.onEnter node=${streamTask.id()} / ${streamTask.hashCode()}")
    Kamon.currentContext()
  }

  @Advice.OnMethodExit(onThrowable = classOf[Throwable], suppress = classOf[Throwable])
  def onExit(@Advice.This streamTask:StreamTask, @Advice.Enter ctx: Context, @Advice.Thrown throwable: Throwable):Unit = {
    println(s"==> StreamTask.onExit node=${streamTask.id()} / ${streamTask.hashCode()}")
    val currentSpan = Kamon.currentSpan
    currentSpan.mark(s"kafka.streams.task.id=${streamTask.id()}")
    currentSpan.tag("kafka.applicationId", streamTask.applicationId())

    if(throwable != null) currentSpan.fail(throwable.getMessage)

    currentSpan.finish()
    Kamon.store(ctx)
  }
}

class NextRecordMethodAdvisor
object NextRecordMethodAdvisor {
  @Advice.OnMethodExit(suppress = classOf[Throwable])
  def onExit(@Advice.Argument(0) recordInfo: RecordInfo, @Advice.Return record: StampedRecord): Unit = {
    if (record != null) {
      val header = Option(record.headers.lastHeader("kamon-context"))

      val currentContext = header.map(h => ContextSpanBinaryEncoder.decode(h.value)).getOrElse(Context.Empty)
      val span = Kamon.spanBuilder("stream")
        .asChildOf(currentContext.get(Span.Key))
        .tag("span.kind", "consumer")
        .tag("kafka.partition", record.partition)
        .tag("kafka.topic", record.topic)
        .tag("kafka.offset", record.offset)
        .tag("kafka.stream.node", recordInfo.node().name().replace('\n', ' '))
        .start()

      Kamon.store(Context.of(Span.Key, span))
    }
  }
}

