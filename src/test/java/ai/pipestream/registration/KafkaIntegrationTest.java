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
import java.util.Map;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

/**
 * Integration test for Kafka messaging with Protobuf.
 * The quarkus-apicurio-registry-protobuf extension auto-detects Protobuf types
 * and configures serializers/deserializers automatically.
 */
@QuarkusTest
public class KafkaIntegrationTest {

    private static final Logger LOG = Logger.getLogger(KafkaIntegrationTest.class);

    // Extension auto-detects ServiceRegistered extends MessageLite
    // and configures ProtobufKafkaSerializer + UUIDSerializer
    @SuppressWarnings("CdiInjectionPointsInspection")
    @Inject
    @ProtobufChannel("test-events-out")
    ProtobufEmitter<ServiceRegistered> emitter;

    @Inject
    TestConsumer consumer;

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String kafkaBootstrapServers;

    @ConfigProperty(name = "mp.messaging.outgoing.test-events-out.bootstrap.servers", defaultValue = "not-set")
    String outgoingBootstrapServers;

    private void logDebug(String hypothesisId, String location, String message, Map<String, Object> data) {
        // #region agent log
        StringBuilder logMsg = new StringBuilder();
        logMsg.append("[HYPOTHESIS-").append(hypothesisId).append("] ").append(message);
        if (location != null) {
            logMsg.append(" @ ").append(location);
        }
        if (data != null && !data.isEmpty()) {
            logMsg.append(" | Data: ");
            boolean first = true;
            for (Map.Entry<String, Object> entry : data.entrySet()) {
                if (!first) logMsg.append(", ");
                logMsg.append(entry.getKey()).append("=").append(entry.getValue());
                first = false;
            }
        }
        LOG.info(logMsg.toString());
        // #endregion
    }

    @Test
    public void testEndToEnd() throws InterruptedException, ExecutionException, TimeoutException {
        // #region agent log
        logDebug("A", "KafkaIntegrationTest.java:36", "Test starting", Map.of(
            "kafkaBootstrapServers", kafkaBootstrapServers,
            "outgoingBootstrapServers", outgoingBootstrapServers,
            "dockerGatewayHost", System.getenv("DOCKER_GATEWAY_HOST"),
            "testStartTime", System.currentTimeMillis()
        ));
        // #endregion

        ServiceRegistered msg = ServiceRegistered.newBuilder()
                .setServiceName("test-service")
                .setServiceId("test-id")
                .build();

        // #region agent log
        logDebug("B", "KafkaIntegrationTest.java:42", "Before emitter.send", Map.of(
            "messageBuilt", true,
            "serviceName", msg.getServiceName(),
            "serviceId", msg.getServiceId()
        ));
        // #endregion

        CompletableFuture<Void> sendFuture = emitter.send(msg).toCompletableFuture();

        // #region agent log
        long getStartTime = System.currentTimeMillis();
        logDebug("C", "KafkaIntegrationTest.java:45", "Send future created", Map.of(
            "sendFutureDone", sendFuture.isDone(),
            "sendFutureCancelled", sendFuture.isCancelled(),
            "timeBeforeGet", getStartTime
        ));
        // #endregion

        try {
            sendFuture.get(10, TimeUnit.SECONDS);

            // #region agent log
            logDebug("D", "KafkaIntegrationTest.java:51", "Send completed successfully", Map.of(
                "timeToComplete", System.currentTimeMillis() - getStartTime
            ));
            // #endregion
        } catch (TimeoutException e) {
            // #region agent log
            logDebug("E", "KafkaIntegrationTest.java:55", "Send timed out", Map.of(
                "timeoutAfter", System.currentTimeMillis() - getStartTime,
                "sendFutureDone", sendFuture.isDone(),
                "sendFutureCancelled", sendFuture.isCancelled(),
                "exceptionMessage", e.getMessage()
            ));
            // #endregion
            throw e;
        } catch (Exception e) {
            // #region agent log
            logDebug("F", "KafkaIntegrationTest.java:63", "Send failed with exception", Map.of(
                "exceptionType", e.getClass().getName(),
                "exceptionMessage", e.getMessage(),
                "sendFutureDone", sendFuture.isDone()
            ));
            // #endregion
            throw e;
        }

        // #region agent log
        logDebug("G", "KafkaIntegrationTest.java:70", "Waiting for consumer", Map.of(
            "consumerReceivedDone", consumer.getReceived().isDone()
        ));
        // #endregion

        // Wait for consumer
        ServiceRegistered received = consumer.getReceived().get(10, TimeUnit.SECONDS);

        // #region agent log
        logDebug("H", "KafkaIntegrationTest.java:75", "Consumer received message", Map.of(
            "receivedNotNull", received != null,
            "receivedServiceName", (received != null) ? received.getServiceName() : null
        ));
        // #endregion

        assertThat("Received message should not be null", received, notNullValue());
        assertThat("Service Name should match", received.getServiceName(), equalTo("test-service"));
        assertThat("Service ID should match", received.getServiceId(), equalTo("test-id"));
    }

    @jakarta.enterprise.context.ApplicationScoped
    public static class TestConsumer {
        private final CompletableFuture<ServiceRegistered> received = new CompletableFuture<>();
        private static final Logger LOG = Logger.getLogger(TestConsumer.class);

        private void logDebug(String hypothesisId, String location, String message, Map<String, Object> data) {
            // #region agent log
            StringBuilder logMsg = new StringBuilder();
            logMsg.append("[HYPOTHESIS-").append(hypothesisId).append("] ").append(message);
            if (location != null) {
                logMsg.append(" @ ").append(location);
            }
            if (data != null && !data.isEmpty()) {
                logMsg.append(" | Data: ");
                boolean first = true;
                for (Map.Entry<String, Object> entry : data.entrySet()) {
                    if (!first) logMsg.append(", ");
                    logMsg.append(entry.getKey()).append("=").append(entry.getValue());
                    first = false;
                }
            }
            LOG.info(logMsg.toString());
            // #endregion
        }

        // Extension auto-detects ServiceRegistered parameter type
        // and configures ProtobufKafkaDeserializer + UUIDDeserializer
        @Incoming("test-events-in")
        public void consume(ServiceRegistered msg) {
            // #region agent log
            logDebug("I", "KafkaIntegrationTest.java:95", "Consumer received message", Map.of(
                "serviceName", msg.getServiceName(),
                "serviceId", msg.getServiceId(),
                "receivedWasDone", received.isDone()
            ));
            // #endregion
            received.complete(msg);

            // #region agent log
            logDebug("J", "KafkaIntegrationTest.java:103", "Consumer completed future", Map.of(
                "receivedNowDone", received.isDone()
            ));
            // #endregion
        }

        public CompletableFuture<ServiceRegistered> getReceived() {
            return received;
        }
    }
}
