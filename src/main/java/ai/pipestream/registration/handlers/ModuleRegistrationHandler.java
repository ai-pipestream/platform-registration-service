package ai.pipestream.registration.handlers;

import com.google.protobuf.Timestamp;
import ai.pipestream.data.module.v1.GetServiceRegistrationRequest;
import ai.pipestream.data.module.v1.GetServiceRegistrationResponse;
import ai.pipestream.platform.registration.v1.*;
import ai.pipestream.platform.registration.client.RegistrationGrpcClients;
import ai.pipestream.registration.consul.ConsulHealthChecker;
import ai.pipestream.registration.consul.ConsulRegistrar;
import ai.pipestream.registration.entity.ServiceModule;
import ai.pipestream.registration.events.OpenSearchEventsProducer;
import ai.pipestream.registration.repository.ApicurioRegistryClient;
import ai.pipestream.registration.repository.ModuleRepository;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.unchecked.Unchecked;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.smallrye.common.vertx.VertxContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Handles module registration operations with proper reactive flow.
 * Updated to use the unified RegisterRequest from the new proto API.
 */
@ApplicationScoped
public class ModuleRegistrationHandler {

    private static final Logger LOG = Logger.getLogger(ModuleRegistrationHandler.class);

    @Inject
    ConsulRegistrar consulRegistrar;

    @Inject
    ConsulHealthChecker healthChecker;

    @Inject
    ModuleRepository moduleRepository;

    @Inject
    ApicurioRegistryClient apicurioClient;

    @Inject
    RegistrationGrpcClients grpcClients;

    @Inject
    OpenSearchEventsProducer openSearchProducer;

    /**
     * Register a module with streaming status updates.
     * Expects RegisterRequest with type=SERVICE_TYPE_MODULE.
     * Flow: Validate → Consul → Health → Fetch Metadata → Apicurio → Database → OpenSearch
     * Returns Multi&lt;RegistrationEvent&gt; which PlatformRegistrationService wraps in RegisterResponse.
     */
    public Multi<RegistrationEvent> registerModule(RegisterRequest request) {
        Connectivity connectivity = request.getConnectivity();
        String host = connectivity.getAdvertisedHost();
        int port = connectivity.getAdvertisedPort();
        String moduleName = request.getName();
        String serviceId = ConsulRegistrar.generateServiceId(moduleName, host, port);

        // Start with validation as a Uni
        return Uni.createFrom().item(Unchecked.supplier(() -> {
            if (!validateModuleRequest(request)) {
                throw new IllegalArgumentException("Invalid module registration request: Missing required fields");
            }
            return request;
        }))
        .onItem().transformToMulti(validatedRequest -> {
            // Create a Multi that emits events throughout the registration process
            return Multi.createBy().concatenating()
                .streams(
                    Multi.createFrom().item(createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_STARTED, "Starting module registration", serviceId)),
                    Multi.createFrom().item(createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_VALIDATED, "Module registration request validated", null)),
                    executeModuleRegistrationAsMulti(validatedRequest, serviceId)
                );
        })
        .onFailure().recoverWithMulti(error -> {
            LOG.error("Module registration failed", error);
            return Multi.createFrom().items(
                createEventWithError(serviceId, "Registration failed", error.getMessage())
            );
        });
    }

    private Multi<RegistrationEvent> executeModuleRegistrationAsMulti(RegisterRequest request, String serviceId) {
        return consulRegistrar.registerService(request, serviceId)
            .onItem().transformToMulti(consulSuccess -> {
                if (!consulSuccess) {
                    return Multi.createFrom().item(
                        createEventWithError(serviceId, "Failed to register with Consul", "Consul registration failed")
                    );
                }

                return Multi.createBy().concatenating().streams(
                    Multi.createFrom().item(createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_CONSUL_REGISTERED, "Module registered with Consul", serviceId)),
                    Multi.createFrom().item(createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_HEALTH_CHECK_CONFIGURED, "Health check configured", null)),
                    continueRegistrationFlow(request, serviceId)
                );
            });
    }

    private Multi<RegistrationEvent> continueRegistrationFlow(RegisterRequest request, String serviceId) {
        return healthChecker.waitForHealthy(request.getName(), serviceId)
            .onItem().transformToMulti(healthy -> {
                if (!healthy) {
                    return rollbackConsulRegistration(serviceId)
                        .onItem().transformToMulti(v -> Multi.createFrom().item(
                            createEventWithError(serviceId, "Module failed health checks",
                                "Module did not become healthy within timeout period")
                        ));
                }

                return Multi.createBy().concatenating().streams(
                    Multi.createFrom().item(createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_CONSUL_HEALTHY, "Module reported healthy by Consul", null)),
                    completeRegistrationFlow(request, serviceId)
                );
            });
    }

    private Multi<RegistrationEvent> completeRegistrationFlow(RegisterRequest request, String serviceId) {
        Connectivity connectivity = request.getConnectivity();
        String host = connectivity.getAdvertisedHost();
        int port = connectivity.getAdvertisedPort();
        String moduleName = request.getName();
        String version = request.getVersion();

        return fetchModuleMetadata(request)
            .chain(metadata -> {
                String schema = extractOrSynthesizeSchema(metadata, moduleName);
                Map<String, Object> metadataMap = buildMetadataMap(metadata);

                // Create a duplicated (safe) Vert.x context and switch the downstream onto it
                Context safeCtx = VertxContext.createNewDuplicatedContext();

                return Uni.createFrom().item(1)
                    .emitOn(r -> safeCtx.runOnContext(x -> r.run()))
                    .invoke(() -> {
                        Context ctx = Vertx.currentContext();
                        boolean duplicated = ctx != null && VertxContext.isDuplicatedContext(ctx);
                        LOG.debugf("DB segment context: present=%s, duplicated=%s, thread=%s",
                                ctx != null, duplicated, Thread.currentThread().getName());
                    })
                    .chain(ignored -> moduleRepository.registerModule(
                        moduleName,
                        host,
                        port,
                        version,
                        metadataMap,
                        schema
                    ))
                    .map(savedModule -> new DatabaseSaveContext(savedModule, metadata, schema));
            })
            // After database is done, we can call Apicurio on worker thread
            .chain(dbContext -> apicurioClient.createOrUpdateSchema(
                    moduleName,
                    request.getVersion(),
                    dbContext.schema
                )
                .map(schemaResult -> new SavedContext(dbContext.module, schemaResult))
                .onFailure().recoverWithItem(err -> {
                    LOG.warnf(err, "Apicurio registration failed for %s:%s, continuing without registry sync",
                            moduleName, request.getVersion());
                    return new SavedContext(dbContext.module, null);
                }))
            .onItem().transformToMulti(savedContext -> {
                // Emit to OpenSearch (fire and forget)
                openSearchProducer.emitModuleRegistered(
                    savedContext.module.serviceId,
                    moduleName,
                    host,
                    port,
                    version,
                    savedContext.module.configSchemaId,
                    savedContext.schemaResult != null ? savedContext.schemaResult.getArtifactId() : null
                );

                return Multi.createFrom().items(
                    createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_METADATA_RETRIEVED, "Module metadata retrieved", null),
                    createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_SCHEMA_VALIDATED, "Schema validated or synthesized", null),
                    createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_DATABASE_SAVED, "Module registration saved to database", savedContext.module.serviceId),
                    (savedContext.schemaResult != null
                        ? createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_APICURIO_REGISTERED, "Schema registered in Apicurio", null)
                        : createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_SCHEMA_VALIDATED, "Apicurio registry sync skipped (failure)", null)
                    ),
                    createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_COMPLETED, "Module registration completed successfully", savedContext.module.serviceId)
                );
            });
    }

    // Helper classes to pass context through the chain
    private static class DatabaseSaveContext {
        final ServiceModule module;
        final GetServiceRegistrationResponse metadata;
        final String schema;

        DatabaseSaveContext(ServiceModule module, GetServiceRegistrationResponse metadata, String schema) {
            this.module = module;
            this.metadata = metadata;
            this.schema = schema;
        }
    }

    private static class SavedContext {
        final ServiceModule module;
        final ApicurioRegistryClient.SchemaRegistrationResponse schemaResult;

        SavedContext(ServiceModule module, ApicurioRegistryClient.SchemaRegistrationResponse schemaResult) {
            this.module = module;
            this.schemaResult = schemaResult;
        }
    }


    /**
     * Unregister a module using the unified UnregisterRequest
     */
    public Uni<UnregisterResponse> unregisterModule(UnregisterRequest request) {
        String serviceId = ConsulRegistrar.generateServiceId(request.getName(), request.getHost(), request.getPort());

        return consulRegistrar.unregisterService(serviceId)
            .map(success -> {
                UnregisterResponse.Builder response = UnregisterResponse.newBuilder()
                    .setSuccess(success)
                    .setTimestamp(createTimestamp());

                if (success) {
                    response.setMessage("Module unregistered successfully");
                    // Emit to OpenSearch
                    openSearchProducer.emitModuleUnregistered(serviceId, request.getName());
                } else {
                    response.setMessage("Failed to unregister module");
                }

                return response.build();
            });
    }

    private Uni<GetServiceRegistrationResponse> fetchModuleMetadata(RegisterRequest request) {
        String moduleName = request.getName();
        return grpcClients.getPipeStepProcessorClient(moduleName)
            .onItem().transformToUni(stub ->
                stub.getServiceRegistration(GetServiceRegistrationRequest.newBuilder().build())
            );
    }

    private Uni<Void> rollbackConsulRegistration(String serviceId) {
        return consulRegistrar.unregisterService(serviceId)
            .onItem().invoke(success -> {
                if (success) {
                    LOG.infof("Rolled back Consul registration for %s", serviceId);
                } else {
                    LOG.errorf("Failed to rollback Consul registration for %s", serviceId);
                }
            })
            .replaceWith(Uni.createFrom().voidItem());
    }

    private String extractOrSynthesizeSchema(GetServiceRegistrationResponse metadata, String moduleName) {
        if (metadata.hasJsonConfigSchema() && !metadata.getJsonConfigSchema().isBlank()) {
            return metadata.getJsonConfigSchema();
        }

        // Synthesize a default key-value OpenAPI 3.1 schema
        return "{\n" +
            "  \"openapi\": \"3.1.0\",\n" +
            "  \"info\": { \"title\": \"" + moduleName + " Configuration\", \"version\": \"1.0.0\" },\n" +
            "  \"components\": {\n" +
            "    \"schemas\": {\n" +
            "      \"Config\": {\n" +
            "        \"type\": \"object\",\n" +
            "        \"additionalProperties\": { \"type\": \"string\" },\n" +
            "        \"description\": \"Key-value configuration for " + moduleName + "\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
    }

    private Map<String, Object> buildMetadataMap(GetServiceRegistrationResponse metadata) {
        Map<String, Object> map = new HashMap<>(metadata.getMetadataMap());

        if (metadata.hasDisplayName()) map.put("display_name", metadata.getDisplayName());
        if (metadata.hasDescription()) map.put("description", metadata.getDescription());
        if (metadata.hasOwner()) map.put("owner", metadata.getOwner());
        if (metadata.hasDocumentationUrl()) map.put("documentation_url", metadata.getDocumentationUrl());
        if (!metadata.getTagsList().isEmpty()) map.put("tags", metadata.getTagsList());
        if (!metadata.getDependenciesList().isEmpty()) map.put("dependencies", metadata.getDependenciesList());

        return map;
    }

    private boolean validateModuleRequest(RegisterRequest request) {
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
