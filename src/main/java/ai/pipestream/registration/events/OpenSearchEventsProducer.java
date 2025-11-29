package ai.pipestream.registration.events;

import ai.pipestream.platform.registration.ModuleRegistered;
import ai.pipestream.platform.registration.ServiceRegistered;
import ai.pipestream.platform.registration.ModuleUnregistered;
import ai.pipestream.platform.registration.ServiceUnregistered;
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

    public void emitModuleRegistered(String serviceId, String moduleName, String host, int port,
                                    String version, String schemaId, String apicurioArtifactId) {
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

    private Timestamp createTimestamp() {
        long millis = System.currentTimeMillis();
        return Timestamp.newBuilder()
            .setSeconds(millis / 1000)
            .setNanos((int) ((millis % 1000) * 1_000_000))
            .build();
    }
}