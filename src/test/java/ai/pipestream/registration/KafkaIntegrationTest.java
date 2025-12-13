package ai.pipestream.registration;

import io.quarkus.test.junit.QuarkusTest;
import org.junit.jupiter.api.Test;
import jakarta.inject.Inject;
import ai.pipestream.apicurio.registry.protobuf.ProtobufChannel;
import ai.pipestream.apicurio.registry.protobuf.ProtobufEmitter;
import org.eclipse.microprofile.reactive.messaging.Incoming;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import ai.pipestream.platform.registration.v1.ServiceRegistered;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Integration test for Kafka messaging with Protobuf.
 * The quarkus-apicurio-registry-protobuf extension auto-detects Protobuf types
 * and configures serializers/deserializers automatically.
 */
@QuarkusTest
public class KafkaIntegrationTest {

    // Extension auto-detects ServiceRegistered extends MessageLite
    // and configures ProtobufKafkaSerializer + UUIDSerializer
    @SuppressWarnings("CdiInjectionPointsInspection")
    @Inject
    @ProtobufChannel("test-events-out")
    ProtobufEmitter<ServiceRegistered> emitter;

    @Inject
    TestConsumer consumer;

    @Test
    public void testEndToEnd() throws InterruptedException, ExecutionException, TimeoutException {
        ServiceRegistered msg = ServiceRegistered.newBuilder()
                .setServiceName("test-service")
                .setServiceId("test-id")
                .build();

        emitter.send(msg).toCompletableFuture().get(10, TimeUnit.SECONDS);

        // Wait for consumer
        ServiceRegistered received = consumer.getReceived().get(10, TimeUnit.SECONDS);

        assertThat("Received message should not be null", received, notNullValue());
        assertThat("Service Name should match", received.getServiceName(), equalTo("test-service"));
        assertThat("Service ID should match", received.getServiceId(), equalTo("test-id"));
    }

    @jakarta.enterprise.context.ApplicationScoped
    public static class TestConsumer {
        private final CompletableFuture<ServiceRegistered> received = new CompletableFuture<>();

        // Extension auto-detects ServiceRegistered parameter type
        // and configures ProtobufKafkaDeserializer + UUIDDeserializer
        @Incoming("test-events-in")
        public void consume(ServiceRegistered msg) {
            received.complete(msg);
        }

        public CompletableFuture<ServiceRegistered> getReceived() {
            return received;
        }
    }
}
