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
package kamon.kafka.stream.instrumentation.advisor;

import kamon.Kamon;
import kamon.context.Context;
import kamon.kafka.instrumentation.ContextSerializationHelper;
import kamon.kafka.streams.instrumentation.StampedRecordWithSpan;
import kamon.trace.Span;
import kanela.agent.libs.net.bytebuddy.asm.Advice;
import lombok.Value;
import lombok.val;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.streams.processor.internals.PartitionGroup;
import org.apache.kafka.streams.processor.internals.StampedRecord;

@Value
public class Advisors {

    public static class NextRecordMethodAdvisor {

        public static Context getContext(Header h) {
            if(h != null) {
                return ContextSerializationHelper.fromByteArray(h.value());
            } else {
                return Context.Empty();
            }
        }

        @Advice.OnMethodExit(suppress = Throwable.class)
        public static void  onExit(@Advice.Argument(0) PartitionGroup.RecordInfo recordInfo, @Advice.Return(readOnly = false) StampedRecord record) {
            if (record != null) {
                val header = record.headers().lastHeader("kamon-context");
                val currentContext = getContext(header);
                val span = Kamon.spanBuilder("stream")
                        .asChildOf(currentContext.get(Span.Key()))
                        .tag("span.kind", "consumer")
                        .tag("kafka.partition", record.partition())
                        .tag("kafka.topic", record.topic())
                        .tag("kafka.offset", record.offset())
                        .tag("kafka.stream.node", recordInfo.node().name().replace('\n', ' '))
                        .start();
                Kamon.storeContext(Context.of(Span.Key(), span));
                record = new StampedRecordWithSpan(record, span);
            }
        }
    }
}

