package ai.pipestream.registration;

import ai.pipestream.api.annotation.ProtobufIncoming;
import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import jakarta.inject.Inject;
import org.eclipse.microprofile.reactive.messaging.Channel;
import io.smallrye.reactive.messaging.MutinyEmitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import ai.pipestream.platform.registration.ServiceRegistered;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

@QuarkusTest
public class KafkaIntegrationTest {

    @Inject
    @Channel("test-events-out")
    @ai.pipestream.api.annotation.ProtobufChannel("test-events-out")
    MutinyEmitter<ServiceRegistered> emitter;

    @Inject
    TestConsumer consumer;

    @Test
    public void testEndToEnd() throws InterruptedException, ExecutionException, TimeoutException {
        ServiceRegistered msg = ServiceRegistered.newBuilder()
                .setServiceName("test-service")
                .setServiceId("test-id")
                .build();

        emitter.sendAndAwait(msg);

        // Wait for consumer
        ServiceRegistered received = consumer.getReceived().get(10, TimeUnit.SECONDS);

        assertThat("Received message should not be null", received, notNullValue());
        assertThat("Service Name should match", received.getServiceName(), equalTo("test-service"));
        assertThat("Service ID should match", received.getServiceId(), equalTo("test-id"));
    }

    @jakarta.enterprise.context.ApplicationScoped
    public static class TestConsumer {
        private final CompletableFuture<ServiceRegistered> received = new CompletableFuture<>();

        // The extension automatically configures the deserializer and maps to topic
        // "test-events"
        @Incoming("test-events-in")
        @ProtobufIncoming("test-events-in")
        public void consume(ServiceRegistered msg) {
            received.complete(msg);
        }

        public CompletableFuture<ServiceRegistered> getReceived() {
            return received;
        }
    }
}
