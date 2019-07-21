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
import org.apache.kafka.streams.processor.internals.{StampedRecord, StreamTask}


class StreamsInstrumentation extends InstrumentationBuilder {
  /**
    * Instruments org.apache.kafka.streams.processor.internals.StreamTask::process
    */
  onType("org.apache.kafka.streams.processor.internals.StreamTask")
    .advise(method("process"), classOf[ProcessMethodAdvisor])

  /**
    * Instruments org.apache.kafka.streams.processor.internals.PartitionGroup::nextRecord
    */
  onType("org.apache.kafka.streams.processor.internals.PartitionGroup")
    .advise(method("nextRecord").and(withReturnTypes(classOf[org.apache.kafka.streams.processor.internals.StampedRecord])), classOf[NextRecordMethodAdvisor])
}

class ProcessMethodAdvisor
object ProcessMethodAdvisor {
  @Advice.OnMethodEnter
  def onEnter(): Context = Kamon.currentContext()

  @Advice.OnMethodExit(onThrowable = classOf[Throwable], suppress = classOf[Throwable])
  def onExit(@Advice.This streamTask:StreamTask, @Advice.Enter ctx: Context, @Advice.Thrown throwable: Throwable):Unit = {
    val currentSpan = Kamon.currentSpan
    currentSpan.mark(s"kafka.streams.task.id=${streamTask.id()}")

    if(throwable != null) currentSpan.fail(throwable.getMessage)

    currentSpan.finish()
    Kamon.store(ctx)
  }
}

class NextRecordMethodAdvisor
object NextRecordMethodAdvisor {
  @Advice.OnMethodExit(suppress = classOf[Throwable])
  def onExit(@Advice.Return record: StampedRecord): Unit = {
    if (record != null) {
      val header = Option(record.headers.lastHeader("kamon-context"))
      val currentContext = header.map(h => ContextSpanBinaryEncoder.decode(h.value)).getOrElse(Context.Empty)

      val span = Kamon.spanBuilder("stream")
        .asChildOf(currentContext.get(Span.Key))
        .tag("span.kind", "consumer")
        .tag("kafka.partition", record.partition)
        .tag("kafka.topic", record.topic)
        .tag("kafka.offset", record.offset)
        .start()

      Kamon.store(Context.of(Span.Key, span))
    }
  }
}

