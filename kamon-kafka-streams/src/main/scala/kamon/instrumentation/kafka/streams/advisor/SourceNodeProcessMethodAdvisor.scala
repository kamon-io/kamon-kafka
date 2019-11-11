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
import kamon.instrumentation.context.HasContext
import kanela.agent.libs.net.bytebuddy.asm.Advice
import org.apache.kafka.streams.processor.internals.SourceNode

/**
  * org.apache.kafka.streams.processor.internals.SourceNode
  * public void process(final K key, final V value)
  *
  * This advise is invoked after the "parent" advise of ProcessorNode
  */
class SourceNodeProcessMethodAdvisor
object SourceNodeProcessMethodAdvisor extends NodeTraceSupport {

  @Advice.OnMethodEnter
  def onEnter[K,V](@Advice.This node: SourceNode[_,_] with HasProcessorContextWithKamonContext with HasContext, @Advice.Argument(0) key: K, @Advice.Argument(1) value: V): Unit = {
    val pCtx = extractProcessorContext(node)
    Kamon.currentSpan().tagMetrics("kafka.source.topic", pCtx.recordContext().topic())
    Kamon.currentSpan().tagMetrics("kafka.source.key", key.toString)
  }
}
