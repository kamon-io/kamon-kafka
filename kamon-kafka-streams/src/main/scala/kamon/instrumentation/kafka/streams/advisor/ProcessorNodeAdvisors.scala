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
import kamon.context.Storage.Scope
import kamon.instrumentation.context.HasContext
import kamon.trace.Span
import kanela.agent.libs.net.bytebuddy.asm.Advice
import org.apache.kafka.streams.processor.internals.{InternalProcessorContext, ProcessorNode}

/**
  * org.apache.kafka.streams.processor.internals.ProcessorNode
  * public void init (final InternalProcessorContext context)
  */
class ProcessorNodeInitMethodAdvisor
object ProcessorNodeInitMethodAdvisor extends NodeTraceSupport {

  @Advice.OnMethodEnter
  def onEnter(@Advice.This node: ProcessorNode[_,_] with HasProcessorContextWithKamonContext with HasContext, @Advice.Argument(0) ctx: InternalProcessorContext with HasContext): Unit = {
    // Store processor context on our node
    node.setProcessorContext(Some(ctx))
  }

}

/**
  * org.apache.kafka.streams.processor.internals.ProcessorNode
  * public void process(final K key, final V value)
  */
class ProcessorNodeProcessMethodAdvisor
object ProcessorNodeProcessMethodAdvisor extends NodeTraceSupport {

  @Advice.OnMethodEnter
  def onEnter(@Advice.This node: ProcessorNode[_,_] with HasProcessorContextWithKamonContext with HasContext): Scope = {
    val pCtx = extractProcessorContext(node)
    val previousSpan = Kamon.currentSpan()
    // Determine the context to use as `currentContext` during the execution of `process`
    val newCurrentContext = if (shouldTrace(pCtx)) {
      // create a new span for this node
      val spanBuilder = Kamon.spanBuilder(pCtx.currentNode().name())
        .asChildOf(pCtx.context.get(Span.Key))
        .tagMetrics("span.kind", "processor")
        .tagMetrics("component", "kafka.stream.node")

      // link to the "previous" node of the stream topology - if there is one
      if(previousSpan != Span.Empty)
        spanBuilder.link(previousSpan, Span.Link.Kind.FollowsFrom)

      val span = spanBuilder.start()
      val nodeCtx = Context.of(Span.Key, span)
      node.setContext(nodeCtx)
      nodeCtx
    } else
    // use the stream context if it exists, otherwise fall back to Context.empty
      pCtx.context

    // Store/set context while executing the actual `process` function
    Kamon.storeContext(newCurrentContext)
  }

  @Advice.OnMethodExit(onThrowable = classOf[Throwable]) // todo: add "suppress = classOf[Throwable]" ?
  def onExit(@Advice.This node: ProcessorNode[_,_] with HasProcessorContextWithKamonContext with HasContext, @Advice.Enter scope: Scope, @Advice.Thrown throwable: Throwable): Unit = {

    val pCtx = extractProcessorContext(node)

    if (shouldTrace(pCtx)) {
      assert(node.context != null, "Expect node context to be available!")
      // Log exception if needed
      Option(throwable).foreach(t => node.context.get(Span.Key).fail(t))
      // Finish span and reset node context
      node.context.get(Span.Key).finish()
      node.setContext(null)
    } else {
      // Log exception on stream's context if needed
      Option(throwable).foreach(t => pCtx.context.get(Span.Key).fail(t))
    }

    // Close the scope to remove the current context
    scope.close()
  }
}


/**
  * org.apache.kafka.streams.processor.internals.ProcessorNode
  * public void close()
  */
class ProcessorNodeCloseMethodAdvisor
object ProcessorNodeCloseMethodAdvisor extends NodeTraceSupport {

  @Advice.OnMethodExit
  def onExit(@Advice.This node: ProcessorNode[_,_] with HasProcessorContextWithKamonContext with HasContext): Unit = {
    // Close the scope to remove the current context and reset the stored pCtx on the node
    node.setProcessorContext(None)
  }
}

