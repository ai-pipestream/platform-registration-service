package ai.pipestream.registration.handlers;

import com.google.protobuf.Timestamp;
import ai.pipestream.platform.registration.v1.*;
import ai.pipestream.registration.consul.ConsulHealthChecker;
import ai.pipestream.registration.consul.ConsulRegistrar;
import ai.pipestream.registration.events.OpenSearchEventsProducer;
import ai.pipestream.registration.repository.ApicurioRegistryClient;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

/**
 * Handles service registration operations.
 * Updated to use the unified RegisterRequest from the new proto API.
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
     * Returns Multi&lt;RegistrationEvent&gt; which PlatformRegistrationService wraps in RegisterResponse.
     */
    public Multi<RegistrationEvent> registerService(RegisterRequest request) {
        Connectivity connectivity = request.getConnectivity();
        String host = connectivity.getAdvertisedHost();
        int port = connectivity.getAdvertisedPort();
        String serviceId = ConsulRegistrar.generateServiceId(request.getName(), host, port);

        return Multi.createFrom().emitter(emitter -> {
            // Emit STARTED event
            emitter.emit(createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_STARTED, "Starting service registration", serviceId));

            // Validate request
            if (!validateServiceRequest(request)) {
                RegistrationEvent failed = createEventWithError(serviceId, "Invalid service registration request", "Missing required fields");
                emitter.emit(failed);
                emitter.complete();
                return;
            }

            emitter.emit(createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_VALIDATED, "Service registration request validated", null));

            // Register with Consul
            consulRegistrar.registerService(request, serviceId)
                .onItem().transformToUni(success -> {
                    if (!success) {
                        RegistrationEvent failed = createEventWithError(serviceId, "Failed to register with Consul", "Consul registration returned false");
                        emitter.emit(failed);
                        emitter.complete();
                        return Uni.createFrom().voidItem();
                    }

                    emitter.emit(createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_CONSUL_REGISTERED, "Service registered with Consul", serviceId));
                    emitter.emit(createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_HEALTH_CHECK_CONFIGURED, "Health check configured", null));

                    // Wait for health check
                    return healthChecker.waitForHealthy(request.getName(), serviceId)
                        .onItem().transformToUni(healthy -> {
                            if (!healthy) {
                                // Service registered but never became healthy
                                RegistrationEvent failed = createEventWithError(serviceId, "Service registered but failed health checks",
                                    "Service did not become healthy within timeout period. Check service logs and connectivity.");
                                emitter.emit(failed);

                                // Cleanup - unregister from Consul
                                consulRegistrar.unregisterService(serviceId)
                                    .subscribe().with(
                                        cleanup -> LOG.debugf("Cleaned up unhealthy service registration: %s", serviceId),
                                        error -> LOG.errorf(error, "Failed to cleanup unhealthy service: %s", serviceId)
                                    );

                                emitter.complete();
                                return Uni.createFrom().voidItem();
                            }

                            emitter.emit(createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_CONSUL_HEALTHY, "Service reported healthy by Consul", null));

                            Uni<Void> schemaRegistration = Uni.createFrom().voidItem();
                            if (request.hasHttpSchema()) {
                                String schemaVersion = request.hasHttpSchemaVersion() && !request.getHttpSchemaVersion().isBlank()
                                    ? request.getHttpSchemaVersion()
                                    : request.getVersion();
                                String artifactBase = request.hasHttpSchemaArtifactId() && !request.getHttpSchemaArtifactId().isBlank()
                                    ? request.getHttpSchemaArtifactId()
                                    : request.getName() + "-http";

                                schemaRegistration = apicurioClient
                                    .createOrUpdateSchemaWithArtifactBase(artifactBase, schemaVersion, request.getHttpSchema())
                                    .invoke(result -> emitter.emit(
                                        createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_APICURIO_REGISTERED,
                                            "HTTP schema registered in Apicurio", null)))
                                    .onFailure().invoke(error ->
                                        LOG.warnf(error, "Failed to register HTTP schema for service %s", request.getName()))
                                    .replaceWithVoid();
                            }

                            return schemaRegistration.invoke(() -> {
                                RegistrationEvent completed = createEvent(
                                    PlatformEventType.PLATFORM_EVENT_TYPE_COMPLETED,
                                    "Service registration completed successfully",
                                    serviceId);
                                emitter.emit(completed);

                                // Emit to OpenSearch on success
                                openSearchProducer.emitServiceRegistered(serviceId, request.getName(),
                                    host, port, request.getVersion());
                                emitter.complete();
                            });
                        });
                })
                .subscribe().with(
                    result -> {}, // Success handled above
                    error -> {
                        LOG.error("Failed to register service", error);
                        RegistrationEvent failed = createEventWithError(serviceId, "Registration failed", error.getMessage());
                        emitter.emit(failed);
                        emitter.complete();
                    }
                );
        });
    }

    /**
     * Unregister a service using the unified UnregisterRequest
     */
    public Uni<UnregisterResponse> unregisterService(UnregisterRequest request) {
        String serviceId = ConsulRegistrar.generateServiceId(request.getName(), request.getHost(), request.getPort());

        return consulRegistrar.unregisterService(serviceId)
            .map(success -> {
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
            });
    }

    private boolean validateServiceRequest(RegisterRequest request) {
        if (request.getName().isEmpty()) {
            return false;
        }
        if (!request.hasConnectivity()) {
            return false;
        }
        Connectivity conn = request.getConnectivity();
        return !conn.getAdvertisedHost().isEmpty() && conn.getAdvertisedPort() > 0;
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
