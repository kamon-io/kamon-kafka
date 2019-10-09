/* =========================================================================================
 * Copyright (C) 2013-2019 the kamon project <http://kamon.io/>
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
import kamon.kafka.client.instrumentation.ContextSerializationHelper;
import kamon.kafka.client.instrumentation.ProducerCallback;
import kamon.trace.Span;
import kanela.agent.libs.net.bytebuddy.asm.Advice;
import lombok.val;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Producer Instrumentation
 */
public class SendMethodAdvisor {
    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static void onEnter(@Advice.Argument(value = 0, readOnly = false) ProducerRecord record,
                               @Advice.Argument(value = 1, readOnly = false) Callback callback,
                               @Advice.Local("scope") Storage.Scope scope) {
        val currentContext = Kamon.currentContext();
        val topic = record.topic() == null ? "kafka" : record.topic();
        val partition = record.partition() == null ? "unknown-partition" : record.partition().toString();
        val value = record.key() == null ? "unknown-key" : record.key().toString();

        val span = Kamon.producerSpanBuilder("send", "kafka.producer")
                .tag("kafka.key", value)
                .tag("kafka.partition", partition)
                .tag("kafka.topic", topic)
                .start();

        val ctx = currentContext.withEntry(Span.Key(), span);
        record.headers().add("kamon-context", ContextSerializationHelper.toByteArray(ctx));

        scope = Kamon.storeContext(ctx);
        callback = new ProducerCallback(callback, scope);

    }

    @Advice.OnMethodExit(onThrowable = Throwable.class, suppress = Throwable.class)
    public static void onExit(@Advice.Local("scope") Storage.Scope scope,
                              @Advice.Thrown final Throwable throwable) {

        val span = scope.context().get(Span.Key());
        if (throwable != null) span.fail(throwable.getMessage(), throwable);
        span.finish();
        scope.close();
    }
}
