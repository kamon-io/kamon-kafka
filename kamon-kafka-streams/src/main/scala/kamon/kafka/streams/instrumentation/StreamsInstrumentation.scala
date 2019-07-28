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
import kamon.kafka.stream.instrumentation.advisor.Advisors.NextRecordMethodAdvisor
import kamon.trace.Span
import kanela.agent.api.instrumentation.InstrumentationBuilder
import kanela.agent.libs.net.bytebuddy.asm.Advice
import kanela.agent.libs.net.bytebuddy.asm.Advice.{FieldValue, Local}
import kanela.agent.libs.net.bytebuddy.jar.asm.commons.Method
import lombok.Value
import org.apache.kafka.streams.processor.internals.PartitionGroup.RecordInfo
import org.apache.kafka.streams.processor.internals.{ProcessorNode, StampedRecord, StreamTask}


class StreamsInstrumentation extends InstrumentationBuilder {

  println("======> StreamsInstrumentation running")

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
    val currentSpan = Kamon.currentSpan()
    println(s"==> ProcessorNode.onEnter node=${node.name()} / hashCode=${node.hashCode()} / currentSpan=${currentSpan.id} / parentSpan=${currentSpan.parentId}")
    val span = Kamon.spanBuilder("node")
      .asChildOf(currentSpan)
      .tag("span.kind", "processor")
      .tag("kafka.stream.node", node.name())
      .start()

    println(s"    -> NEW SPAN currentSpan=${span.id} / parentSpan=${span.parentId}")
    Context.of(Span.Key, span)
  }

  @Advice.OnMethodExit(onThrowable = classOf[Throwable], suppress = classOf[Throwable])
  def onExit(@Advice.This node: ProcessorNode[_,_], @Advice.Enter ctx: Context, @Advice.Thrown throwable: Throwable):Unit = {
    val currentSpan = ctx.get(Span.Key)
    println(s"==> ProcessorNode.onExit node=${node.name()} / ${node.hashCode()}/  currentSpan=${currentSpan.id} / parentSpan=${currentSpan.parentId}")
    if(throwable != null) currentSpan.fail(throwable.getMessage)
    println(s"   -> finishing ${currentSpan.id}")
    currentSpan.finish()
//    Kamon.store(ctx)
  }
}

class ProcessMethodAdvisor
object ProcessMethodAdvisor {
  @Advice.OnMethodEnter
  def onEnter(@Advice.This streamTask:StreamTask): Context = {
    println(s"==> StreamTask.onEnter node=${streamTask.id()} / ${streamTask.hashCode()}")
    Kamon.currentContext() // todo: Why should this be required since it seems to contain only Span.Empty?
  }

  @Advice.OnMethodExit(onThrowable = classOf[Throwable], suppress = classOf[Throwable])
  def onExit(@Advice.Origin r: Any, @Advice.This streamTask:StreamTask, @Advice.Enter ctx: Context, @Advice.Thrown throwable: Throwable):Unit = {

    val currentSpan = Kamon.currentSpan() //ctx.get(Span.Key)
    println(s"==> StreamTask.onExit node=${streamTask.id()} / hash=${streamTask.hashCode()} / currentSpan=${currentSpan.id} / parentSpan=${currentSpan.parentId}")
    currentSpan.mark(s"kafka.streams.task.id=${streamTask.id()}")
    currentSpan.tag("kafka.applicationId", streamTask.applicationId())

    if(throwable != null) currentSpan.fail(throwable.getMessage)
    println(s"   -> finishing ${currentSpan.id}")
    currentSpan.finish() // todo: This is potentially performed multiple times if a NULL record was fetched
//    Kamon.store(ctx)
  }
}

class StampedRecordWithSpan(record: StampedRecord, val span: Span) extends StampedRecord(record.value, record.timestamp)

