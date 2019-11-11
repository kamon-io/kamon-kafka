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
import kamon.instrumentation.kafka.streams.Streams
import org.apache.kafka.streams.processor.internals.InternalProcessorContext

trait NodeTraceSupport {
  def extractProcessorContext(x: HasProcessorContextWithKamonContext): InternalProcessorContext with HasContext = {
    assert(x.processorContext.nonEmpty, "Expect processor context to be available!")
    x.processorContext.get
  }

  def shouldTrace(processorContext: InternalProcessorContext with  HasContext): Boolean =
    Streams.traceNodes && Kamon.filter(Streams.StreamsTraceFilterName).accept(processorContext.applicationId())
}
