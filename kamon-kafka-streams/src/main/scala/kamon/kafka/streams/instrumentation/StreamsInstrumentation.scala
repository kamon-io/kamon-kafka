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
import kamon.trace.Span
import kanela.agent.libs.net.bytebuddy.asm.Advice
import kanela.agent.scala.KanelaInstrumentation
import org.apache.kafka.streams.processor.internals.{StampedRecord, StreamTask}


class StreamsInstrumentation extends KanelaInstrumentation {
  /**
    * Instruments org.apache.kafka.streams.processor.internals.StreamTask::process
    */
  forTargetType("org.apache.kafka.streams.processor.internals.StreamTask") { builder =>
    builder
      .withAdvisorFor(method("process"), classOf[ProcessMethodAdvisor])
      .build()
  }

  /**
    * Instruments org.apache.kafka.streams.processor.internals.PartitionGroup::nextRecord
    */
  forTargetType("org.apache.kafka.streams.processor.internals.PartitionGroup") { builder =>
    builder
      .withAdvisorFor(method("nextRecord").and(withReturnTypes(classOf[org.apache.kafka.streams.processor.internals.StampedRecord])), classOf[NextRecordMethodAdvisor])
      .build()
  }
}

class ProcessMethodAdvisor
object ProcessMethodAdvisor {
  @Advice.OnMethodEnter
  def enter(): Context =
    Kamon.currentContext()

  @Advice.OnMethodExit(onThrowable = classOf[Throwable], suppress = classOf[Throwable])
  def exit(@Advice.This obj:StreamTask, @Advice.Enter ctx: Context, @Advice.Thrown throwable: Throwable):Unit = {
    val currentSpan = Kamon.currentSpan

    currentSpan.mark(s"kafka.streams.task.id=${obj.asInstanceOf[StreamTask].id()}")

    if(throwable != null)
      currentSpan.addError(throwable.getMessage).finish()

    currentSpan.finish()
    Kamon.storeContext(ctx)
  }
}

class NextRecordMethodAdvisor
object NextRecordMethodAdvisor {
  @Advice.OnMethodExit(suppress = classOf[Throwable])
  def startSpan(@Advice.Return record: StampedRecord): Unit = {
    if (record != null) {
      val header = Option(record.headers.lastHeader("kamon-context"))
      val currentContext = header.map(h => Kamon.contextCodec.Binary.decode(ByteBuffer.wrap(h.value))).getOrElse(Context.Empty)

      val span = Kamon.buildSpan("stream")
        .asChildOf(currentContext.get(Span.ContextKey))
        .withMetricTag("span.kind", "consumer")
        .withTag("kafka.partition", record.partition)
        .withTag("kafka.topic", record.topic)
        .withTag("kafka.offset", record.offset)
        .start()

      Kamon.storeContext(Context.create(Span.ContextKey, span))
    }
  }
}

