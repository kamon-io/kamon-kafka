package kamon.kafka.client.instrumentation.advisor;

import kamon.Kamon;
import kamon.kafka.client.instrumentation.RecordProcessor;
import kanela.agent.libs.net.bytebuddy.asm.Advice;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Instant;

/**
 * Consumer Instrumentation
 */
public class PollMethodAdvisor {
    @Advice.OnMethodEnter(suppress = Throwable.class)
    public static void onEnter(@Advice.Local("startTime") Instant startTime) {
        startTime = Kamon.clock().instant();
    }

    @Advice.OnMethodExit(suppress = Throwable.class)
    public static <K, V> void onExit(@Advice.Local("startTime") Instant startTime, @Advice.FieldValue("groupId") String groupId, @Advice.FieldValue("clientId") String clientId, @Advice.Return(readOnly = false) ConsumerRecords<K, V> records) {
        records = RecordProcessor.process(startTime, clientId, groupId, records);
    }
}