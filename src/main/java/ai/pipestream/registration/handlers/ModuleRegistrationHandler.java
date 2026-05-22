package ai.pipestream.registration.handlers;

import com.google.protobuf.Timestamp;
import ai.pipestream.platform.registration.v1.*;
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
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
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
    OpenSearchEventsProducer openSearchProducer;

    @Inject
    Vertx vertx;

    private WebClient webClient;

    @jakarta.annotation.PostConstruct
    void init() {
        WebClientOptions options = new WebClientOptions()
                .setConnectTimeout(5000)
                .setIdleTimeout(10);
        this.webClient = WebClient.create(vertx, options);
    }

    /**
     * Register a module with streaming status updates.
     * Expects RegisterRequest with type=SERVICE_TYPE_MODULE and inline {@code module} metadata.
     * Flow: Validate → Consul → Health → Apicurio → Database → OpenSearch
     */
    public Multi<RegistrationEvent> registerModule(RegisterRequest request) {
        Connectivity connectivity = request.getConnectivity();
        String host = connectivity.getAdvertisedHost();
        int port = connectivity.getAdvertisedPort();
        String moduleName = request.getName();
        String serviceId = ConsulRegistrar.generateServiceId(moduleName, host, port);

        return Uni.createFrom().item(Unchecked.supplier(() -> {
            ValidationResult validation = RegisterRequestValidator.validateModuleRequest(request);
            if (!validation.valid()) {
                throw new IllegalArgumentException("Invalid module registration request: " + validation.reasonsAsString());
            }
            return request;
        }))
        .onItem().transformToMulti(validatedRequest -> Multi.createBy().concatenating()
                .streams(
                    Multi.createFrom().item(createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_STARTED, "Starting module registration", serviceId)),
                    Multi.createFrom().item(createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_VALIDATED, "Module registration request validated", null)),
                    executeModuleRegistrationAsMulti(validatedRequest, serviceId)
                ))
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
        String version = RegistrationVersionSanitizer.sanitize(
                request.getVersion(), moduleName, LOG, "register_request.version");
        ModuleRegistration moduleMetadata = request.getModule();

        if (!moduleMetadata.getHealthCheckPassed()) {
            String detail = moduleMetadata.hasHealthCheckMessage()
                    ? moduleMetadata.getHealthCheckMessage()
                    : "Module reported health_check_passed=false";
            return Multi.createFrom().item(
                    createEventWithError(serviceId, "Module health check failed", detail));
        }

        String schema = extractOrSynthesizeSchema(moduleMetadata, moduleName);
        Map<String, Object> metadataMap = buildMetadataMap(moduleMetadata);

        Context safeCtx = VertxContext.createNewDuplicatedContext();

        return Uni.createFrom().item(1)
            .emitOn(r -> safeCtx.runOnContext(x -> r.run()))
            .chain(ignored -> moduleRepository.registerModule(
                    moduleName,
                    host,
                    port,
                    version,
                    metadataMap,
                    schema
            ))
            .map(savedModule -> new DatabaseSaveContext(savedModule, schema))
            .chain(dbContext -> {
                Uni<ApicurioRegistryClient.SchemaRegistrationResponse> configSchemaUni =
                    apicurioClient.createOrUpdateSchema(moduleName, version, dbContext.schema)
                        .onFailure().recoverWithItem(err -> {
                            LOG.warnf(err, "Apicurio config schema registration failed for %s:%s, continuing without registry sync",
                                    moduleName, version);
                            return null;
                        });

                Uni<ApicurioRegistryClient.SchemaRegistrationResponse> apiSchemaUni =
                    fetchOpenApiSpec(host, port, request)
                        .chain(openApiSpec -> {
                            if (openApiSpec != null && !openApiSpec.isBlank()) {
                                LOG.infof("Registering OpenAPI spec for %s:%s to Apicurio", moduleName, version);
                                return apicurioClient.createOrUpdateSchemaWithArtifactBase(
                                        moduleName + "-api", version, openApiSpec)
                                    .onFailure().recoverWithItem(err -> {
                                        LOG.warnf(err, "Apicurio API schema registration failed for %s:%s, continuing without registry sync",
                                                moduleName, version);
                                        return null;
                                    });
                            }
                            LOG.debugf("No OpenAPI spec available for %s:%s, skipping API schema registration",
                                    moduleName, version);
                            return Uni.createFrom().nullItem();
                        });

                return Uni.combine().all().unis(configSchemaUni, apiSchemaUni).asTuple()
                    .map(tuple -> new SavedContext(dbContext.module, tuple.getItem1(), tuple.getItem2()));
            })
            .onItem().transformToMulti(savedContext -> {
                openSearchProducer.emitModuleRegistered(
                    savedContext.module.serviceId,
                    moduleName,
                    host,
                    port,
                    version,
                    savedContext.module.configSchemaId,
                    savedContext.configSchemaResult != null ? savedContext.configSchemaResult.getArtifactId() : null
                );

                return Multi.createFrom().items(
                    createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_METADATA_RETRIEVED, "Module metadata retrieved", null),
                    createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_SCHEMA_VALIDATED, "Schema validated or synthesized", null),
                    createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_DATABASE_SAVED, "Module registration saved to database", savedContext.module.serviceId),
                    (savedContext.configSchemaResult != null
                        ? createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_APICURIO_REGISTERED, "Config schema registered in Apicurio", null)
                        : createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_SCHEMA_VALIDATED, "Apicurio config schema sync skipped (failure)", null)
                    ),
                    createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_COMPLETED, "Module registration completed successfully", savedContext.module.serviceId)
                );
            });
    }

    private static class DatabaseSaveContext {
        final ServiceModule module;
        final String schema;

        DatabaseSaveContext(ServiceModule module, String schema) {
            this.module = module;
            this.schema = schema;
        }
    }

    private static class SavedContext {
        final ServiceModule module;
        final ApicurioRegistryClient.SchemaRegistrationResponse configSchemaResult;
        final ApicurioRegistryClient.SchemaRegistrationResponse apiSchemaResult;

        SavedContext(ServiceModule module,
                    ApicurioRegistryClient.SchemaRegistrationResponse configSchemaResult,
                    ApicurioRegistryClient.SchemaRegistrationResponse apiSchemaResult) {
            this.module = module;
            this.configSchemaResult = configSchemaResult;
            this.apiSchemaResult = apiSchemaResult;
        }
    }

    public Uni<UnregisterResponse> unregisterModule(UnregisterRequest request) {
        String serviceId = ConsulRegistrar.generateServiceId(request.getName(), request.getHost(), request.getPort());

        return consulRegistrar.unregisterService(serviceId)
            .map(success -> {
                UnregisterResponse.Builder response = UnregisterResponse.newBuilder()
                    .setSuccess(success)
                    .setTimestamp(createTimestamp());

                if (success) {
                    response.setMessage("Module unregistered successfully");
                    openSearchProducer.emitModuleUnregistered(serviceId, request.getName());
                } else {
                    response.setMessage("Failed to unregister module");
                }

                return response.build();
            });
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

    private String extractOrSynthesizeSchema(ModuleRegistration metadata, String moduleName) {
        if (metadata.hasJsonConfigSchema() && !metadata.getJsonConfigSchema().isBlank()) {
            return metadata.getJsonConfigSchema();
        }

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

    private Map<String, Object> buildMetadataMap(ModuleRegistration metadata) {
        Map<String, Object> map = new HashMap<>(metadata.getModuleMetadataMap());

        if (metadata.hasDisplayName()) map.put("display_name", metadata.getDisplayName());
        if (metadata.hasDescription()) map.put("description", metadata.getDescription());
        if (metadata.hasOwner()) map.put("owner", metadata.getOwner());
        if (metadata.hasDocumentationUrl()) map.put("documentation_url", metadata.getDocumentationUrl());
        if (!metadata.getModuleTagsList().isEmpty()) map.put("tags", metadata.getModuleTagsList());
        if (!metadata.getDependenciesList().isEmpty()) map.put("dependencies", metadata.getDependenciesList());
        if (metadata.hasExpectedMaxProcessingSeconds()) {
            map.put("expected_max_processing_seconds", metadata.getExpectedMaxProcessingSeconds());
        }
        if (metadata.hasHealthCheckMessage()) {
            map.put("health_check_message", metadata.getHealthCheckMessage());
        }

        return map;
    }

    private Uni<String> fetchOpenApiSpec(String host, int port, RegisterRequest request) {
        String openApiPath = "/q/openapi";
        int httpPort = port;
        String httpHost = host;

        if (!request.getHttpEndpointsList().isEmpty()) {
            HttpEndpoint endpoint = request.getHttpEndpoints(0);
            if (!endpoint.getHost().isBlank()) {
                httpHost = endpoint.getHost();
            }
            if (endpoint.getPort() > 0) {
                httpPort = endpoint.getPort();
            }
            if (!endpoint.getBasePath().isBlank() && !"/".equals(endpoint.getBasePath())) {
                String base = endpoint.getBasePath();
                if (!base.endsWith("/")) {
                    base = base + "/";
                }
                openApiPath = base + "q/openapi";
            }
        }

        final String path = openApiPath;
        final String resolvedHost = httpHost;
        final int resolvedPort = httpPort;

        return Uni.createFrom().completionStage(() ->
            webClient.get(resolvedPort, resolvedHost, path)
                    .timeout(5000)
                    .send()
                    .toCompletionStage()
        ).map(response -> {
            if (response.statusCode() == 200) {
                String spec = response.bodyAsString();
                LOG.debugf("Fetched OpenAPI spec from %s:%d (%d bytes)", resolvedHost, resolvedPort, spec.length());
                return spec;
            }
            LOG.debugf("OpenAPI endpoint returned %d for %s:%d, skipping API schema registration",
                    (Object) response.statusCode(), resolvedHost, resolvedPort);
            return null;
        }).onFailure().recoverWithItem(err -> {
            LOG.debugf("OpenAPI endpoint not available at %s:%d, skipping API schema registration: %s",
                    resolvedHost, resolvedPort, err.getMessage());
            return null;
        });
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
            .setErrorDetail(errorDetail != null ? errorDetail : "")
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
