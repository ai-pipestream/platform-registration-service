package ai.pipestream.registration.topics;

import io.quarkus.virtual.threads.VirtualThreads;
import io.smallrye.mutiny.Uni;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Creates the per-service module-work Kafka topic at module-registration
 * time. One topic per module service:
 * {@code pipestream.module.<moduleName>} — no cluster/graph/node (routing
 * identity rides the PipeStream). Single-owner by construction
 * (registration runs in platform-registration-service), idempotent, and
 * created with explicit partitions (NOT broker-default auto-create).
 *
 * <p>Mirrors the engine's {@code KafkaTopicRegistrar.ensureProcessingTopic}
 * idempotent pattern (listTopics → skip if present → createTopics). The
 * blocking AdminClient work runs on Quarkus virtual threads so it composes
 * concurrently with the Apicurio schema-registration unis without
 * blocking the Vert.x event loop. Topic-ensure failure is isolated by
 * the caller — module registration must not fail because Kafka hiccuped.
 */
@ApplicationScoped
public class ModuleTopicProvisioner {

    private static final Logger LOG = Logger.getLogger(ModuleTopicProvisioner.class);

    /** Module-work topic prefix — matches the engine's ModuleServiceTopics contract. */
    static final String PREFIX = "pipestream.module.";

    @ConfigProperty(name = "kafka.bootstrap.servers")
    String bootstrapServers;

    @ConfigProperty(name = "pipestream.kafka.default-partitions", defaultValue = "4")
    int defaultPartitions;

    @ConfigProperty(name = "pipestream.kafka.default-replication-factor", defaultValue = "1")
    short defaultReplicationFactor;

    @Inject
    @VirtualThreads
    java.util.concurrent.Executor virtualThreadExecutor;

    private AdminClient adminClient;

    @PostConstruct
    void init() {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 5000);
        config.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10000);
        adminClient = AdminClient.create(config);
    }

    @PreDestroy
    void close() {
        if (adminClient != null) {
            adminClient.close();
        }
    }

    /** The module-work topic name. Strict: blank module name fails loud. */
    static String moduleTopic(String moduleName) {
        if (moduleName == null || moduleName.isBlank()) {
            throw new IllegalArgumentException(
                    "cannot derive module-work topic: blank module name");
        }
        return PREFIX + moduleName;
    }

    /** Idempotent decision: create only if the topic is not already present. */
    static boolean shouldCreate(Set<String> existingTopics, String topic) {
        return !existingTopics.contains(topic);
    }

    /**
     * Idempotently ensure {@code pipestream.module.<moduleName>} exists,
     * off the event loop. Completes normally whether the topic was
     * created or already existed.
     */
    public Uni<Void> ensureModuleTopic(String moduleName) {
        String topic = moduleTopic(moduleName);
        return Uni.createFrom().<Void>item(() -> {
            try {
                Set<String> existing = adminClient.listTopics().names().get(10, TimeUnit.SECONDS);
                if (shouldCreate(existing, topic)) {
                    adminClient.createTopics(List.of(
                            new NewTopic(topic, defaultPartitions, defaultReplicationFactor)))
                            .all().get(10, TimeUnit.SECONDS);
                    LOG.infof("Created module-work topic %s (partitions=%d, replication=%d)",
                            topic, defaultPartitions, defaultReplicationFactor);
                } else {
                    LOG.debugf("Module-work topic %s already exists; no-op", topic);
                }
            } catch (Exception e) {
                // Rethrow so the caller's onFailure().recover isolates it —
                // registration must not fail because topic-ensure hiccuped.
                throw new RuntimeException(
                        "ensureModuleTopic failed for " + topic + ": " + e.getMessage(), e);
            }
            return null;
        }).runSubscriptionOn(virtualThreadExecutor);
    }
}
