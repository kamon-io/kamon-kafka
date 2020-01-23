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

  /*TODO
  *  Apply mixin to RecordContext, post construct
  * use internal headers received from record to construct initial context
  * Every processor can get currentContext and from there Kamon context via record context, no need to propagate it manually
  * Especially if its immediately burned into headers
  * */


  /**
    * This is required to access the original ConsumerRecord wrapped by StampedRecord
    * in order to extract his span. This span can then be used a parent for the
    * span representing the stream.
    */
  onSubTypesOf("org.apache.kafka.streams.processor.internals.StampedRecord")
    .mixin(classOf[Mixin])
    .advise(isConstructor, classOf[StampedRecordAdvisor]) //TODO no instrumentation neccessary, stampedRecord is Stamped<ConsumerRecord>, cast and access public member?
  //TODO bleh, ambiguous overloads, also not sure why span is set, fix others first

  /**
    * This propagates the span from the original "raw" record to the "deserialized" record
    * that is processed by the stream.
    */
  onType("org.apache.kafka.streams.processor.internals.RecordDeserializer") //TODO replace this with RecordContext instrumentation
    .advise(method("deserialize"), classOf[RecordDeserializerAdvisor])

  /**
    * Instrument KStream's AbstractProcessor.process for creating node-specifc spans.
    * This can be enabled/disable via configuration.
    * Also provide a bridge to access the internal processor context which carries
    * the Kamon context.
    */ //TODO ProcessorNode is inited only to pickup metric info and then inits Processor with context, node and internalContext are irelevant, only Processor & processorContext
  onSubTypesOf("org.apache.kafka.streams.processor.internals.ProcessorNode")   //TODO why processorNode even, why not just processor?
    .advise(method("init"), classOf[ProcessorNodeInitMethodAdvisor]) //TODO Node is inited with processorContext which carries kamon context, why (sinks and sources???), done once per lifecycle
    .advise(method("process"), classOf[ProcessorNodeProcessMethodAdvisor]) //TODO this is Processor.process + metrics recording, why not only processor
    .advise(method("close"), classOf[ProcessorNodeCloseMethodAdvisor])
    .mixin(classOf[HasContext.VolatileMixin])
    .mixin(classOf[HasProcessorContextWithKamonContext.Mixin]) //TODO could this be bridget since its there anyway

  onType("org.apache.kafka.streams.processor.internals.SinkNode")
    .advise(method("process"), classOf[SinkNodeProcessMethodAdvisor])  //TODO this one is caught both by ProcessorNode and SinkNode, only tags metrics but what about the ordering, maybe create a sinking operation?

  onType("org.apache.kafka.streams.processor.internals.SourceNode")
    .advise(method("process"), classOf[SourceNodeProcessMethodAdvisor])

  //TODO should either trace stages or entire subtopology

  /**
    * Instruments org.apache.kafka.streams.processor.internals.StreamTask::updateProcessorContext
    *
    * UpdateProcessContextAdvisor: this will start the stream span
    * ProcessMethodAdvisor: this will finish the stream span
    *
    */
  onType("org.apache.kafka.streams.processor.internals.StreamTask") //TODO is this supposed to be parent span for all stream processing stage spans
    .advise(method("updateProcessorContext"), classOf[StreamTaskUpdateProcessContextAdvisor]) //TODO this creates a span for this processing
    .advise(method("process"), classOf[StreamTaskProcessMethodAdvisor]) //TODO finishes span started in updateContext, and tags with infor available from StreamTask


  /*Instrument RecordContext to carry span
  * Processor.process will access super.context and get message context from there
  * Need to get task info into record context too
  * */



  /**
    * Keep the stream's Kamon context on the (Abstract)ProcessorContext so that it is available
    * to all participants of this stream's processing.
    */
  onSubTypesOf("org.apache.kafka.streams.processor.internals.AbstractProcessorContext") //TODO why would processor context need kamon context, like current message processing
    .mixin(classOf[HasContext.VolatileMixin])

  /**
    * Propagate the Kamon context from ProcessorContext to the RecordCollector.
    */
  onSubTypesOf("org.apache.kafka.streams.processor.internals.RecordCollector$Supplier")
    .advise(method("recordCollector"), classOf[RecordCollectorSupplierAdvisor]) //TODO copies context from processor, but once processor is changed, is collector left with old context?

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


  //TODO in all advices, having stage filtered out from tracing breaks context propagation
  //TODO should move all context propagation to RecordContext and ProcessorContext respectively

}
