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
import kamon.context.Storage.Scope
import kamon.instrumentation.context.HasContext
import kanela.agent.libs.net.bytebuddy.asm.Advice
import org.apache.kafka.streams.processor.internals.RecordCollector

class RecordCollectorSendAdvisor
object RecordCollectorSendAdvisor {
  @Advice.OnMethodEnter
  def onEnter(@Advice.This collector: RecordCollector with HasContext): Scope = {
    Kamon.storeContext(collector.context)
  }
  @Advice.OnMethodExit
  def onExit(@Advice.This collector: RecordCollector with HasContext,
            @Advice.Enter scope: Scope): Unit = {
    scope.close()
    collector.setContext(null)
  }
}