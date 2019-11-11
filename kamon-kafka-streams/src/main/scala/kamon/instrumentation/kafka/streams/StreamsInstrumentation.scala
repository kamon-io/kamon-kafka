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
package kamon.instrumentation.kafka.streams

import kamon.instrumentation.context.HasContext
import kamon.instrumentation.kafka.streams.advisor.HasConsumerRecord.Mixin
import kamon.instrumentation.kafka.streams.advisor._
import kanela.agent.api.instrumentation.InstrumentationBuilder

class StreamsInstrumentation extends InstrumentationBuilder {

  /**
    * This is required to access the original ConsumerRecord wrapped by StampedRecord
    * in order to extract his span. This span can then be used a parent for the
    * span representing the stream.
    */
  onSubTypesOf("org.apache.kafka.streams.processor.internals.StampedRecord")
    .mixin(classOf[Mixin])
    .advise(isConstructor, classOf[StampedRecordAdvisor])

  /**
    * This propagates the span from the original "raw" record to the "deserialized" record
    * that is processed by the stream.
    */
  onType("org.apache.kafka.streams.processor.internals.RecordDeserializer")
    .advise(method("deserialize"), classOf[RecordDeserializerAdvisor])

  /**
    * Instrument KStream's AbstractProcessor.process for creating node-specifc spans.
    * This can be enabled/disable via configuration.
    * Also provide a bridge to access the internal processor context which carries
    * the Kamon context.
    */
  onSubTypesOf("org.apache.kafka.streams.processor.internals.ProcessorNode")
    .advise(method("init"), classOf[ProcessorNodeInitMethodAdvisor])
    .advise(method("process"), classOf[ProcessorNodeProcessMethodAdvisor])
    .advise(method("close"), classOf[ProcessorNodeCloseMethodAdvisor])
    .mixin(classOf[HasContext.VolatileMixin])
    .mixin(classOf[HasProcessorContextWithKamonContext.Mixin])

  onType("org.apache.kafka.streams.processor.internals.SinkNode")
    .advise(method("process"), classOf[SinkNodeProcessMethodAdvisor])

  onType("org.apache.kafka.streams.processor.internals.SourceNode")
    .advise(method("process"), classOf[SourceNodeProcessMethodAdvisor])

  /**
    * Instruments org.apache.kafka.streams.processor.internals.StreamTask::updateProcessorContext
    *
    * UpdateProcessContextAdvisor: this will start the stream span
    * ProcessMethodAdvisor: this will finish the stream span
    *
    */
  onType("org.apache.kafka.streams.processor.internals.StreamTask")
    .advise(method("updateProcessorContext"), classOf[StreamTaskUpdateProcessContextAdvisor])
    .advise(method("process"), classOf[StreamTaskProcessMethodAdvisor])

  /**
    * Keep the stream's Kamon context on the (Abstract)ProcessorContext so that it is available
    * to all participants of this stream's processing.
    */
  onSubTypesOf("org.apache.kafka.streams.processor.internals.AbstractProcessorContext")
    .mixin(classOf[HasContext.VolatileMixin])

  /**
    * Propagate the Kamon context from ProcessorContext to the RecordCollector.
    */
  onSubTypesOf("org.apache.kafka.streams.processor.internals.RecordCollector$Supplier")
    .advise(method("recordCollector"), classOf[RecordCollectorSupplierAdvisor])

  /**
    * Have the Kamon context available, store it when invoking the send method so that it can be picked
    * up by the ProducerInstrumentation and close it after the call.
    *
    * It must match the correct version of the send method since there are two with different signatures,
    * one delegating to the other. Without the match on the 4th argument both send methods would be instrumented.
    */
  onSubTypesOf("org.apache.kafka.streams.processor.internals.RecordCollector")
    .mixin(classOf[HasContext.VolatileMixin])
    .advise(method("send").and(withArgument(4, classOf[Integer])), classOf[RecordCollectorSendAdvisor])

}
