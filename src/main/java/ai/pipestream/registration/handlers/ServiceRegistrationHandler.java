package ai.pipestream.registration.handlers;

import com.google.protobuf.Timestamp;
import ai.pipestream.platform.registration.v1.*;
import ai.pipestream.registration.consul.ConsulHealthChecker;
import ai.pipestream.registration.consul.ConsulRegistrar;
import ai.pipestream.registration.events.OpenSearchEventsProducer;
import ai.pipestream.registration.repository.ApicurioRegistryClient;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.function.Consumer;

/**
 * Handles service registration operations.
 *
 * <p>Blocking — registration streams {@link RegistrationEvent}s to the supplied sink as each
 * step completes. The external Consul APIs are Mutiny-only, so their {@code Uni} results are
 * bridged to blocking with {@code .await().indefinitely()} on the caller's virtual thread.
 */
@ApplicationScoped
public class ServiceRegistrationHandler {

    private static final Logger LOG = Logger.getLogger(ServiceRegistrationHandler.class);

    @Inject
    ConsulRegistrar consulRegistrar;

    @Inject
    ConsulHealthChecker healthChecker;

    @Inject
    OpenSearchEventsProducer openSearchProducer;

    @Inject
    ApicurioRegistryClient apicurioClient;

    /**
     * Register a service with streaming status updates.
     * Expects RegisterRequest with type=SERVICE_TYPE_SERVICE.
     * Emits each {@link RegistrationEvent} to {@code sink} as the flow progresses.
     */
    public void registerService(RegisterRequest request, Consumer<RegistrationEvent> sink) {
        Connectivity connectivity = request.getConnectivity();
        String host = connectivity.getAdvertisedHost();
        int port = connectivity.getAdvertisedPort();
        String sanitizedVersion = RegistrationVersionSanitizer.sanitize(
            request.getVersion(), request.getName(), LOG, "register_request.version");
        String serviceId = ConsulRegistrar.generateServiceId(request.getName(), host, port);

        try {
            // Emit STARTED event
            sink.accept(createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_STARTED, "Starting service registration", serviceId));

            // Validate request
            ValidationResult validation = RegisterRequestValidator.validateServiceRequest(request);
            if (!validation.valid()) {
                sink.accept(createEventWithError(serviceId, "Invalid service registration request", validation.reasonsAsString()));
                return;
            }

            sink.accept(createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_VALIDATED, "Service registration request validated", null));

            // Register with Consul
            boolean consulSuccess = UniBlocking.await(consulRegistrar.registerService(request, serviceId));
            if (!consulSuccess) {
                sink.accept(createEventWithError(serviceId, "Failed to register with Consul", "Consul registration returned false"));
                return;
            }

            sink.accept(createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_CONSUL_REGISTERED, "Service registered with Consul", serviceId));
            sink.accept(createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_HEALTH_CHECK_CONFIGURED, "Health check configured", null));

            // Wait for health check
            boolean healthy = UniBlocking.await(healthChecker.waitForHealthy(request.getName(), serviceId));
            if (!healthy) {
                // Service registered but never became healthy
                sink.accept(createEventWithError(serviceId, "Service registered but failed health checks",
                    "Service did not become healthy within timeout period. Check service logs and connectivity."));

                // Cleanup - unregister from Consul
                try {
                    UniBlocking.await(consulRegistrar.unregisterService(serviceId));
                    LOG.debugf("Cleaned up unhealthy service registration: %s", serviceId);
                } catch (Exception error) {
                    LOG.errorf(error, "Failed to cleanup unhealthy service: %s", serviceId);
                }
                return;
            }

            sink.accept(createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_CONSUL_HEALTHY, "Service reported healthy by Consul", null));

            if (request.hasHttpSchema()) {
                String schemaVersion = request.hasHttpSchemaVersion() && !request.getHttpSchemaVersion().isBlank()
                    ? RegistrationVersionSanitizer.sanitize(
                        request.getHttpSchemaVersion(), request.getName(), LOG, "register_request.http_schema_version")
                    : sanitizedVersion;
                String artifactBase = request.hasHttpSchemaArtifactId() && !request.getHttpSchemaArtifactId().isBlank()
                    ? request.getHttpSchemaArtifactId()
                    : request.getName() + "-http";

                try {
                    apicurioClient.createOrUpdateSchemaWithArtifactBase(artifactBase, schemaVersion, request.getHttpSchema());
                    sink.accept(createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_APICURIO_REGISTERED,
                        "HTTP schema registered in Apicurio", null));
                } catch (Exception error) {
                    LOG.warnf(error, "Failed to register HTTP schema for service %s", request.getName());
                }
            }

            sink.accept(createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_COMPLETED,
                "Service registration completed successfully", serviceId));

            // Emit to OpenSearch on success
            openSearchProducer.emitServiceRegistered(serviceId, request.getName(), host, port, sanitizedVersion);
        } catch (Exception error) {
            LOG.error("Failed to register service", error);
            sink.accept(createEventWithError(serviceId, "Registration failed", error.getMessage()));
        }
    }

    /**
     * Unregister a service using the unified UnregisterRequest.
     */
    public UnregisterResponse unregisterService(UnregisterRequest request) {
        String serviceId = ConsulRegistrar.generateServiceId(request.getName(), request.getHost(), request.getPort());

        boolean success = UniBlocking.await(consulRegistrar.unregisterService(serviceId));

        UnregisterResponse.Builder response = UnregisterResponse.newBuilder()
            .setSuccess(success)
            .setTimestamp(createTimestamp());

        if (success) {
            response.setMessage("Service unregistered successfully");
            // Emit to OpenSearch
            openSearchProducer.emitServiceUnregistered(serviceId, request.getName());
        } else {
            response.setMessage("Failed to unregister service");
        }

        return response.build();
    }

    private RegistrationEvent createEvent(PlatformEventType type, String message, String serviceId) {
        RegistrationEvent.Builder builder = RegistrationEvent.newBuilder()
            .setEventType(type)
            .setMessage(message)
            .setTimestamp(createTimestamp());

        if (serviceId != null) {
            builder.setServiceId(serviceId);
        }

        return builder.build();
    }

    private RegistrationEvent createEventWithError(String serviceId, String message, String errorDetail) {
        RegistrationEvent.Builder builder = RegistrationEvent.newBuilder()
            .setEventType(PlatformEventType.PLATFORM_EVENT_TYPE_FAILED)
            .setMessage(message)
            .setErrorDetail(errorDetail)
            .setTimestamp(createTimestamp());

        if (serviceId != null) {
            builder.setServiceId(serviceId);
        }

        return builder.build();
    }

    private Timestamp createTimestamp() {
        long millis = System.currentTimeMillis();
        return Timestamp.newBuilder()
            .setSeconds(millis / 1000)
            .setNanos((int) ((millis % 1000) * 1_000_000))
            .build();
    }
}
