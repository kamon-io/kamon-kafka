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
import kamon.instrumentation.kafka.streams.StreamsInstrumentation
import kamon.trace.Span
import kanela.agent.libs.net.bytebuddy.asm.Advice
import org.apache.kafka.streams.processor.internals.InternalProcessorContext
import org.apache.kafka.streams.processor.{AbstractProcessor, ProcessorContext}
import org.slf4j.LoggerFactory

import scala.util.{Failure, Success, Try}

class ProcessorProcessMethodAdvisor
object ProcessorProcessMethodAdvisor {

  // todo: this was initially meant as a safety net if the mixin was not applied, but is it really needed since this and the mixin are part of the same instrumentation?
  def extractContract(pc: ProcessorContext): Option[InternalProcessorContext with HasContext] = {
    Try {
      pc.asInstanceOf[InternalProcessorContext with HasContext]
    } match {
      case Success(p) => Some(p)
      case Failure(t) =>
        val l = LoggerFactory.getLogger(classOf[StreamsInstrumentation])
        l.error("Cannot access concrete implementation InternalProcessorContext with HasContext for: " + pc, t)
        None
    }
  }

  private def shouldTrace(processorContext: Option[InternalProcessorContext with  HasContext]): Boolean =
    Streams.traceNodes && processorContext.map(ctx => Kamon.filter(Streams.StreamsTraceFilterName).accept(ctx.applicationId())).exists(x => x)

  @Advice.OnMethodEnter
  def onEnter(@Advice.This node: AbstractProcessor[_, _] with ProcessorContextBridge): Context = {

    // This may results in None if an unexpected context type is present
    val processorContext = extractContract(node.contextBridge())
    if (shouldTrace(processorContext)) {
      val span = Kamon.spanBuilder(processorContext.fold("Unknown")(_.currentNode().name()))
        .asChildOf(processorContext.fold(Context.Empty)(_.context).get(Span.Key))
        .tagMetrics("span.kind", "processor")
        .tagMetrics("component", "kafka.stream.node")
        .start()
      Context.of(Span.Key, span)
    } else
      Context.Empty
  }

  @Advice.OnMethodExit(onThrowable = classOf[Throwable], suppress = classOf[Throwable])
  def onExit(@Advice.This node: AbstractProcessor[_, _] with ProcessorContextBridge, @Advice.Enter ctx: Context, @Advice.Thrown throwable: Throwable): Unit = {
    if (shouldTrace(extractContract(node.contextBridge()))) {
      val currentSpan = ctx.get(Span.Key)
      if (throwable != null) currentSpan.fail(throwable.getMessage)
      currentSpan.finish()
    }
  }
}
