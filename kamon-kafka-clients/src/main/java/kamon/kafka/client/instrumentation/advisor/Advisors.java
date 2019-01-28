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

package kamon.kafka.client.instrumentation.advisor;

import kamon.Kamon;
import kamon.context.Storage;
import kamon.kafka.instrumentation.ProducerCallback;
import kamon.trace.Span;
import kanela.agent.libs.net.bytebuddy.asm.Advice;
import lombok.Value;
import lombok.val;
import lombok.var;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.LinkedHashMap;

@Value
public class Advisors {

    /**
     * Consumer Instrumentation
     */
    public static class PollMethodAdvisor {
        @Advice.OnMethodExit(suppress = Throwable.class)
        public static <K, V> void onExit(@Advice.Return(readOnly = false) ConsumerRecords<K, V> records) {
            if (!records.isEmpty()) {

                Instant instant = null;

                val consumerSpansForTopic = new LinkedHashMap<String, Span>();

                for (TopicPartition partition : records.partitions()) {
                    val topic = partition.topic();
                    val recordsInPartition = records.records(partition);

                    for (ConsumerRecord<K, V> record : recordsInPartition) {
                        val header = record.headers().lastHeader("kamon-context");
                        val currentContext = Kamon.contextCodec().Binary().decode(ByteBuffer.wrap(header.value()));
                        var span = consumerSpansForTopic.get(topic);

                        if (span == null) {

                            if (instant == null) instant = Kamon.clock().instant();

                            span = Kamon.buildSpan("poll")
                                    .asChildOf(currentContext.get(Span.ContextKey()))
                                    .withMetricTag("span.kind", "consumer")
                                    .withTag("kafka.partition", partition.partition())
                                    .withTag("kafka.topic", topic)
                                    .withFrom(instant)
                                    .start();

                            consumerSpansForTopic.put(topic, span);
                        }

                        val ctx = currentContext.withKey(Span.ContextKey(), span);
                        record.headers().add("kamon-context", Kamon.contextCodec().Binary().encode(ctx).array());
                    }
                }

                consumerSpansForTopic.values().forEach(Span::finish);
            }

            records = records;
        }
    }

    /**
     * Producer Instrumentation
     */
    public static class SendMethodAdvisor {
        @Advice.OnMethodEnter(suppress = Throwable.class)
        public static void onEnter(@Advice.Argument(value = 0, readOnly = false) ProducerRecord record,
                                   @Advice.Argument(value = 1, readOnly = false) Callback callback,
                                   @Advice.Local("scope") Storage.Scope scope) {

            val currentContext = Kamon.currentContext();
            val topic = record.topic() == null ? "kafka" : record.topic();
            val partition = record.partition() == null ? "unknown-partition" : record.partition().toString();
            val value = record.key() == null ? "unknown-key" : record.key().toString();

            val span = Kamon.buildSpan("kafka.produce")
                    .asChildOf(currentContext.get(Span.ContextKey()))
                    .withMetricTag("span.kind", "producer")
                    .withTag("kafka.key", value)
                    .withTag("kafka.partition", partition)
                    .withTag("kafka.topic", topic)
                    .start();

            val ctx = currentContext.withKey(Span.ContextKey(), span);

            record.headers().add("kamon-context", Kamon.contextCodec().Binary().encode(ctx).array());

            scope = Kamon.storeContext(ctx);
            callback = new ProducerCallback(callback, scope);

        }

        @Advice.OnMethodExit(onThrowable = Throwable.class, suppress = Throwable.class)
        public static void onExit(@Advice.Local("scope") Storage.Scope scope,
                                  @Advice.Thrown final Throwable throwable) {

            val span = scope.context().get(Span.ContextKey());
            if (throwable != null) span.addError(throwable.getMessage(), throwable);
            span.finish();
            scope.close();
        }
    }
}









