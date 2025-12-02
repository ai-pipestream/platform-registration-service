package ai.pipestream.registration.events;

import ai.pipestream.platform.registration.v1.ModuleRegistered;
import ai.pipestream.platform.registration.v1.ServiceRegistered;
import ai.pipestream.platform.registration.v1.ModuleUnregistered;
import ai.pipestream.platform.registration.v1.ServiceUnregistered;
import jakarta.enterprise.context.ApplicationScoped;
import org.eclipse.microprofile.reactive.messaging.Channel;
import io.smallrye.reactive.messaging.MutinyEmitter;
import org.jboss.logging.Logger;
import com.google.protobuf.Timestamp;

/**
 * Produces events to Kafka for OpenSearch indexing.
 * Only emits on successful registration/unregistration.
 * Key: UUID (auto-configured by quarkus-apicurio-registry-protobuf extension)
 * Value: Protobuf events (auto-serialized by extension)
 */
@ApplicationScoped
public class OpenSearchEventsProducer {

    private static final Logger LOG = Logger.getLogger(OpenSearchEventsProducer.class);

    // The quarkus-apicurio-registry-protobuf extension auto-detects Protobuf types
    // and configures ProtobufKafkaSerializer + UUIDSerializer automatically

    @Channel("opensearch-service-registered-events-producer")
    MutinyEmitter<ServiceRegistered> serviceRegisteredEmitter;

    @Channel("opensearch-service-unregistered-events-producer")
    MutinyEmitter<ServiceUnregistered> serviceUnregisteredEmitter;

    @Channel("opensearch-module-registered-events-producer")
    MutinyEmitter<ModuleRegistered> moduleRegisteredEmitter;

    @Channel("opensearch-module-unregistered-events-producer")
    MutinyEmitter<ModuleUnregistered> moduleUnregisteredEmitter;

    /**
     * Emits a ServiceRegistered event to Kafka for OpenSearch indexing.
     * @param serviceId the service ID
     * @param serviceName the service name
     * @param host the host
     * @param port the port
     * @param version the version
     */
    public void emitServiceRegistered(String serviceId, String serviceName, String host, int port, String version) {
        try {
            ServiceRegistered event = ServiceRegistered.newBuilder()
                .setServiceId(serviceId)
                .setServiceName(serviceName)
                .setHost(host)
                .setPort(port)
                .setVersion(version)
                .setTimestamp(createTimestamp())
                .build();

            // Extension automatically handles: UUID key generation + Protobuf serialization + Apicurio schema registration
            serviceRegisteredEmitter.send(event);
            LOG.debugf("Emitted ServiceRegistered event for OpenSearch: serviceId=%s", serviceId);
        } catch (Exception e) {
            LOG.warnf(e, "Failed to emit ServiceRegistered event for OpenSearch: %s", serviceId);
        }
    }

    /**
     * Emits a ServiceUnregistered event to Kafka for OpenSearch indexing.
     * @param serviceId the service ID
     * @param serviceName the service name
     */
    public void emitServiceUnregistered(String serviceId, String serviceName) {
        try {
            ServiceUnregistered event = ServiceUnregistered.newBuilder()
                .setServiceId(serviceId)
                .setServiceName(serviceName)
                .setTimestamp(createTimestamp())
                .build();

            // Extension automatically handles: UUID key generation + Protobuf serialization + Apicurio schema registration
            serviceUnregisteredEmitter.send(event);
            LOG.debugf("Emitted ServiceUnregistered event for OpenSearch: serviceId=%s", serviceId);
        } catch (Exception e) {
            LOG.warnf(e, "Failed to emit ServiceUnregistered event for OpenSearch: %s", serviceId);
        }
    }

    /**
     * Emits a ModuleRegistered event to Kafka for OpenSearch indexing.
     * @param serviceId the service ID
     * @param moduleName the module name
     * @param host the host
     * @param port the port
     * @param version the version
     * @param schemaId the schema ID
     * @param apicurioArtifactId the Apicurio artifact ID
     */
    public void emitModuleRegistered(String serviceId, String moduleName, String host, int port, String version, String schemaId, String apicurioArtifactId) {
        try {
            ModuleRegistered event = ModuleRegistered.newBuilder()
                .setServiceId(serviceId)
                .setModuleName(moduleName)
                .setHost(host)
                .setPort(port)
                .setVersion(version)
                .setSchemaId(schemaId)
                .setApicurioArtifactId(apicurioArtifactId)
                .setTimestamp(createTimestamp())
                .build();

            // Extension automatically handles: UUID key generation + Protobuf serialization + Apicurio schema registration
            moduleRegisteredEmitter.send(event);
            LOG.debugf("Emitted ModuleRegistered event for OpenSearch: serviceId=%s", serviceId);
        } catch (Exception e) {
            LOG.warnf(e, "Failed to emit ModuleRegistered event for OpenSearch: %s", serviceId);
        }
    }

    /**
     * Emits a ModuleUnregistered event to Kafka for OpenSearch indexing.
     * @param serviceId the service ID
     * @param moduleName the module name
     */
    public void emitModuleUnregistered(String serviceId, String moduleName) {
        try {
            ModuleUnregistered event = ModuleUnregistered.newBuilder()
                .setServiceId(serviceId)
                .setModuleName(moduleName)
                .setTimestamp(createTimestamp())
                .build();

            // Extension automatically handles: UUID key generation + Protobuf serialization + Apicurio schema registration
            moduleUnregisteredEmitter.send(event);
            LOG.debugf("Emitted ModuleUnregistered event for OpenSearch: serviceId=%s", serviceId);
        } catch (Exception e) {
            LOG.warnf(e, "Failed to emit ModuleUnregistered event for OpenSearch: %s", serviceId);
        }
    }

    /**
     * Creates a Timestamp from current time.
     * @return the Timestamp
     */
    private Timestamp createTimestamp() {
        long millis = System.currentTimeMillis();
        return Timestamp.newBuilder()
            .setSeconds(millis / 1000)
            .setNanos((int) ((millis % 1000) * 1_000_000))
            .build();
    }
}