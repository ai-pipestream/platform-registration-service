package ai.pipestream.registration.handlers;

import com.google.protobuf.Timestamp;
import ai.pipestream.platform.registration.v1.*;
import ai.pipestream.registration.consul.ConsulHealthChecker;
import ai.pipestream.registration.consul.ConsulRegistrar;
import ai.pipestream.registration.entity.ServiceModule;
import ai.pipestream.registration.events.OpenSearchEventsProducer;
import ai.pipestream.registration.repository.ApicurioRegistryClient;
import ai.pipestream.registration.repository.ModuleRepository;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Handles module registration operations.
 *
 * <p>Blocking — registration streams {@link RegistrationEvent}s to the supplied sink as each
 * step completes (validate → Consul → health → Apicurio → database → OpenSearch). The external
 * Consul APIs are Mutiny-only and bridged with {@link UniBlocking}; everything else runs as
 * straight-line blocking code on the caller's virtual thread.
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
     * Flow: Validate → Consul → Health → Apicurio → Database → OpenSearch.
     * Emits each {@link RegistrationEvent} to {@code sink} as the flow progresses.
     */
    public void registerModule(RegisterRequest request, Consumer<RegistrationEvent> sink) {
        Connectivity connectivity = request.getConnectivity();
        String host = connectivity.getAdvertisedHost();
        int port = connectivity.getAdvertisedPort();
        String moduleName = request.getName();
        String serviceId = ConsulRegistrar.generateServiceId(moduleName, host, port);

        try {
            ValidationResult validation = RegisterRequestValidator.validateModuleRequest(request);
            if (!validation.valid()) {
                throw new IllegalArgumentException("Invalid module registration request: " + validation.reasonsAsString());
            }

            sink.accept(createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_STARTED, "Starting module registration", serviceId));
            sink.accept(createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_VALIDATED, "Module registration request validated", null));

            boolean consulSuccess = UniBlocking.await(consulRegistrar.registerService(request, serviceId));
            if (!consulSuccess) {
                sink.accept(createEventWithError(serviceId, "Failed to register with Consul", "Consul registration failed"));
                return;
            }

            sink.accept(createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_CONSUL_REGISTERED, "Module registered with Consul", serviceId));
            sink.accept(createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_HEALTH_CHECK_CONFIGURED, "Health check configured", null));

            boolean healthy = UniBlocking.await(healthChecker.waitForHealthy(moduleName, serviceId));
            if (!healthy) {
                rollbackConsulRegistration(serviceId);
                sink.accept(createEventWithError(serviceId, "Module failed health checks",
                        "Module did not become healthy within timeout period"));
                return;
            }

            sink.accept(createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_CONSUL_HEALTHY, "Module reported healthy by Consul", null));

            completeRegistrationFlow(request, serviceId, sink);
        } catch (Exception error) {
            LOG.error("Module registration failed", error);
            sink.accept(createEventWithError(serviceId, "Registration failed", error.getMessage()));
        }
    }

    private void completeRegistrationFlow(RegisterRequest request, String serviceId, Consumer<RegistrationEvent> sink) {
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
            sink.accept(createEventWithError(serviceId, "Module health check failed", detail));
            return;
        }

        String schema = extractOrSynthesizeSchema(moduleMetadata, moduleName);
        Map<String, Object> metadataMap = buildMetadataMap(moduleMetadata);

        // Persist to the database (blocking, @Transactional in the repository).
        ServiceModule savedModule = moduleRepository.registerModule(
                moduleName, host, port, version, metadataMap, schema);

        // Register the config schema in Apicurio (best effort — DB is the system of record).
        ApicurioRegistryClient.SchemaRegistrationResponse configSchemaResult;
        try {
            configSchemaResult = apicurioClient.createOrUpdateSchema(moduleName, version, schema);
        } catch (Exception err) {
            LOG.warnf(err, "Apicurio config schema registration failed for %s:%s, continuing without registry sync",
                    moduleName, version);
            configSchemaResult = null;
        }

        // Fetch and register the module's OpenAPI spec, if it exposes one (best effort).
        String openApiSpec = fetchOpenApiSpec(host, port, request);
        if (openApiSpec != null && !openApiSpec.isBlank()) {
            LOG.infof("Registering OpenAPI spec for %s:%s to Apicurio", moduleName, version);
            try {
                apicurioClient.createOrUpdateSchemaWithArtifactBase(moduleName + "-api", version, openApiSpec);
            } catch (Exception err) {
                LOG.warnf(err, "Apicurio API schema registration failed for %s:%s, continuing without registry sync",
                        moduleName, version);
            }
        } else {
            LOG.debugf("No OpenAPI spec available for %s:%s, skipping API schema registration",
                    moduleName, version);
        }

        openSearchProducer.emitModuleRegistered(
                savedModule.serviceId,
                moduleName,
                host,
                port,
                version,
                savedModule.configSchemaId,
                configSchemaResult != null ? configSchemaResult.getArtifactId() : null
        );

        sink.accept(createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_METADATA_RETRIEVED, "Module metadata retrieved", null));
        sink.accept(createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_SCHEMA_VALIDATED, "Schema validated or synthesized", null));
        sink.accept(createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_DATABASE_SAVED, "Module registration saved to database", savedModule.serviceId));
        sink.accept(configSchemaResult != null
                ? createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_APICURIO_REGISTERED, "Config schema registered in Apicurio", null)
                : createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_SCHEMA_VALIDATED, "Apicurio config schema sync skipped (failure)", null));
        sink.accept(createEvent(PlatformEventType.PLATFORM_EVENT_TYPE_COMPLETED, "Module registration completed successfully", savedModule.serviceId));
    }

    public UnregisterResponse unregisterModule(UnregisterRequest request) {
        String serviceId = ConsulRegistrar.generateServiceId(request.getName(), request.getHost(), request.getPort());

        boolean success = UniBlocking.await(consulRegistrar.unregisterService(serviceId));

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
    }

    private void rollbackConsulRegistration(String serviceId) {
        // Matches the original reactive flow: a thrown exception propagates to registerModule's
        // handler (→ generic "Registration failed"), while a false result is only logged and the
        // caller continues to emit the accurate "Module failed health checks" event.
        boolean success = UniBlocking.await(consulRegistrar.unregisterService(serviceId));
        if (success) {
            LOG.infof("Rolled back Consul registration for %s", serviceId);
        } else {
            LOG.errorf("Failed to rollback Consul registration for %s", serviceId);
        }
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

    private String fetchOpenApiSpec(String host, int port, RegisterRequest request) {
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

        try {
            var response = webClient.get(resolvedPort, resolvedHost, path)
                    .timeout(5000)
                    .send()
                    .toCompletionStage()
                    .toCompletableFuture()
                    .get();
            if (response.statusCode() == 200) {
                String spec = response.bodyAsString();
                LOG.debugf("Fetched OpenAPI spec from %s:%d (%d bytes)", resolvedHost, resolvedPort, spec.length());
                return spec;
            }
            LOG.debugf("OpenAPI endpoint returned %d for %s:%d, skipping API schema registration",
                    (Object) response.statusCode(), resolvedHost, resolvedPort);
            return null;
        } catch (InterruptedException err) {
            Thread.currentThread().interrupt();
            LOG.debugf("Interrupted fetching OpenAPI spec from %s:%d, skipping API schema registration",
                    resolvedHost, resolvedPort);
            return null;
        } catch (Exception err) {
            LOG.debugf("OpenAPI endpoint not available at %s:%d, skipping API schema registration: %s",
                    resolvedHost, resolvedPort, err.getMessage());
            return null;
        }
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
