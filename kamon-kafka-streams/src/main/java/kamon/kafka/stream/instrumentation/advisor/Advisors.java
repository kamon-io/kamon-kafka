package kamon.kafka.stream.instrumentation.advisor;

import kamon.Kamon;
import kamon.context.Context;
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
                // ugly, ugly, ugly ... :(
                return Kamon.defaultBinaryPropagation().read(kamon.context.BinaryPropagation$ByteStreamReader$.MODULE$.of(h.value()));
            } else {
                return Context.Empty();
            }
        }

        @Advice.OnMethodExit(suppress = Throwable.class)
        public static void  onExit(@Advice.Argument(0) PartitionGroup.RecordInfo recordInfo, @Advice.Return(readOnly = false) StampedRecord record) {
            System.out.println("==> NextRecord.onExit");
            if (record != null) {
                val header = record.headers().lastHeader("kamon-context");
                val currentContext = getContext(header);
                System.out.println("    -> currentSpan="+ Kamon.currentSpan().id().toString() + " / parentSpan=" + Kamon.currentSpan().parentId().toString());
                val span = Kamon.spanBuilder("stream")
                        .asChildOf(currentContext.get(Span.Key()))
                        .tag("span.kind", "consumer")
                        .tag("kafka.partition", record.partition())
                        .tag("kafka.topic", record.topic())
                        .tag("kafka.offset", record.offset())
                        .tag("kafka.stream.node", recordInfo.node().name().replace('\n', ' '))
                        .start();
                System.out.println("    -> NEW SPAN currentSpan=" + span.id().toString() +" / parentSpan=" + span.parentId().toString());
                Kamon.store(Context.of(Span.Key(), span));
                record = new StampedRecordWithSpan(record, span);
            } else {
            }
        }
    }
}

