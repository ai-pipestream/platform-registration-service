package ai.pipestream.registration.repository;

import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.rest.client.models.*;
import io.apicurio.registry.types.ArtifactType;
import io.apicurio.registry.utils.IoUtil;
import io.kiota.http.vertx.VertXRequestAdapter;
import io.smallrye.mutiny.Uni;
import io.smallrye.mutiny.infrastructure.Infrastructure;
import io.vertx.mutiny.core.Vertx;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Client for interacting with Apicurio Registry v3.
 * This is the secondary storage for schemas (primary is MySQL).
 */
@ApplicationScoped
public class ApicurioRegistryClient {

    private static final Logger LOG = Logger.getLogger(ApicurioRegistryClient.class);
    private static final String DEFAULT_GROUP = "ai.pipestream.schemas";

    @Inject
    Vertx vertx;

    @ConfigProperty(name = "apicurio.registry.url", defaultValue = "http://localhost:8081")
    String apicurioUrl;

    private RegistryClient registryClient;
    private VertXRequestAdapter requestAdapter;

    @PostConstruct
    void init() {
        // Initialize the v3 client with VertX adapter
        this.requestAdapter = new VertXRequestAdapter(vertx.getDelegate());

        // Smart URL handling: accept both base URLs and full API endpoint URLs
        String baseUrl;
        if (apicurioUrl.endsWith("/apis/registry/v3")) {
            // Already has the v3 endpoint path - use as-is
            baseUrl = apicurioUrl;
            LOG.infof("Apicurio Registry v3 client initialized with full URL: %s", baseUrl);
        } else if (apicurioUrl.endsWith("/apis/registry/v3/")) {
            // Has v3 endpoint with trailing slash - remove trailing slash
            baseUrl = apicurioUrl.substring(0, apicurioUrl.length() - 1);
            LOG.infof("Apicurio Registry v3 client initialized with full URL (trailing slash removed): %s", baseUrl);
        } else {
            // Base URL only - append the v3 API endpoint path
            baseUrl = apicurioUrl + "/apis/registry/v3";
            LOG.infof("Apicurio Registry v3 client initialized - appended /apis/registry/v3 to base URL: %s", baseUrl);
        }

        this.requestAdapter.setBaseUrl(baseUrl);
        this.registryClient = new RegistryClient(requestAdapter);
    }

    /**
     * Create or update a schema in Apicurio Registry
     */
    public Uni<SchemaRegistrationResponse> createOrUpdateSchema(String serviceName, String version, String jsonSchema) {
        String artifactId = versionedArtifactId(serviceName, version);

        // Execute the blocking operation on a worker thread to avoid blocking the event loop
        return Uni.createFrom().item(() -> {
            try {
                // Create the artifact with the JSON schema
                CreateArtifact createArtifact = new CreateArtifact();
                createArtifact.setArtifactId(artifactId);
                createArtifact.setArtifactType(ArtifactType.JSON);

                // Set up the first version
                CreateVersion firstVersion = new CreateVersion();
                VersionContent content = new VersionContent();
                content.setContent(IoUtil.toString(jsonSchema.getBytes(StandardCharsets.UTF_8)));
                content.setContentType("application/json");
                firstVersion.setContent(content);
                firstVersion.setVersion(version);

                createArtifact.setFirstVersion(firstVersion);

                // Create or update the artifact - THIS IS A BLOCKING CALL
                VersionMetaData versionMetaData = registryClient.groups()
                        .byGroupId(DEFAULT_GROUP)
                        .artifacts()
                        .post(createArtifact, config -> {
                            config.queryParameters.ifExists = IfArtifactExists.FIND_OR_CREATE_VERSION;
                        })
                        .getVersion();

                // Create response
                SchemaRegistrationResponse response = new SchemaRegistrationResponse(
                        artifactId,
                        versionMetaData.getGlobalId(),
                        versionMetaData.getVersion()
                );

                LOG.infof("Successfully registered schema for %s with version %s (globalId: %d)",
                        serviceName, version, versionMetaData.getGlobalId());

                return response;
            } catch (Exception e) {
                // Try to log more details for Apicurio RuleViolation problem details
                try {
                    Class<?> apiExClass = Class.forName("com.microsoft.kiota.ApiException");
                    if (apiExClass.isInstance(e)) {
                        LOG.errorf(e, "Apicurio rejected schema for %s:%s (artifactId=%s)", serviceName, version, artifactId);
                    } else if (e.getCause() != null && apiExClass.isInstance(e.getCause())) {
                        LOG.errorf(e, "Apicurio rejected schema for %s:%s (artifactId=%s)", serviceName, version, artifactId);
                    } else {
                        LOG.errorf(e, "Failed to register schema for service %s:%s (artifactId=%s)", serviceName, version, artifactId);
                    }
                } catch (ClassNotFoundException cnf) {
                    LOG.errorf(e, "Failed to register schema for service %s:%s (artifactId=%s)", serviceName, version, artifactId);
                }
                throw new ApicurioRegistryException(
                    String.format("Failed to register schema for service %s:%s", serviceName, version),
                    serviceName,
                    artifactId,
                    e
                );
            }
        })
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());  // Run on worker thread pool
    }

    /**
     * Get schema by artifact ID
     */
    public Uni<String> getSchema(String serviceName, String version) {
        String artifactId = versionedArtifactId(serviceName, version);

        return Uni.createFrom().item(() -> {
            try {
                // Get the artifact content - THIS IS A BLOCKING CALL
                if (version != null && !version.isEmpty()) {
                    // Get specific version
                    var content = registryClient.groups()
                            .byGroupId(DEFAULT_GROUP)
                            .artifacts()
                            .byArtifactId(artifactId)
                            .versions()
                            .byVersionExpression(version)
                            .content()
                            .get();

                    return IoUtil.toString(content);
                } else {
                    // Get latest version
                    var content = registryClient.groups()
                            .byGroupId(DEFAULT_GROUP)
                            .artifacts()
                            .byArtifactId(artifactId)
                            .versions()
                            .byVersionExpression("latest")
                            .content()
                            .get();

                    return IoUtil.toString(content);
                }
            } catch (Exception e) {
                LOG.errorf(e, "Failed to get schema for service %s", serviceName);
                throw new ApicurioRegistryException(
                    String.format("Failed to get schema for service %s (version: %s)", serviceName, version),
                    serviceName,
                    artifactId,
                    e
                );
            }
        })
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool());  // Run on worker thread pool
    }

    /**
     * Check if Apicurio is healthy
     */
    public Uni<Boolean> isHealthy() {
        return Uni.createFrom().item(() -> {
            try {
                // Try to access the system info endpoint - THIS IS A BLOCKING CALL
                var systemInfo = registryClient.system().info().get();
                return systemInfo != null;
            } catch (Exception e) {
                LOG.debugf("Health check failed: %s", e.getMessage());
                return false;
            }
        })
        .runSubscriptionOn(Infrastructure.getDefaultWorkerPool())  // Run on worker thread pool
        .onFailure().recoverWithItem(false);
    }

    /**
     * List all artifacts in the group (for reconciliation)
     */
    public Uni<List<SearchedArtifact>> listArtifacts() {
        return Uni.createFrom().completionStage(() -> {
            CompletableFuture<List<SearchedArtifact>> future = new CompletableFuture<>();

            try {
                // Search for all artifacts in our group
                ArtifactSearchResults results = registryClient.search()
                        .artifacts()
                        .get(config -> {
                            config.queryParameters.groupId = DEFAULT_GROUP;
                            config.queryParameters.limit = 500;
                            config.queryParameters.offset = 0;
                        });

                future.complete(results.getArtifacts());
            } catch (Exception e) {
                LOG.errorf(e, "Failed to list artifacts");
                future.completeExceptionally(e);
            }

            return future;
        });
    }

    /**
     * Delete an artifact (for cleanup)
     */
    public Uni<Boolean> deleteArtifact(String serviceName) {
        String artifactId = serviceName + "-config";

        return Uni.createFrom().completionStage(() -> {
            CompletableFuture<Boolean> future = new CompletableFuture<>();

            try {
                registryClient.groups()
                        .byGroupId(DEFAULT_GROUP)
                        .artifacts()
                        .byArtifactId(artifactId)
                        .delete();

                LOG.infof("Successfully deleted artifact %s", artifactId);
                future.complete(true);
            } catch (Exception e) {
                LOG.errorf(e, "Failed to delete artifact %s", artifactId);
                future.complete(false);
            }

            return future;
        });
    }

    private String versionedArtifactId(String serviceName, String version) {
        String safeVersion = (version == null || version.isBlank()) ? "v1" : ("v" + version.replace('.', '_'));
        return serviceName + "-config-" + safeVersion;
    }

    /**
     * Get artifact metadata
     * Never returns null - returns a failed Uni if metadata cannot be retrieved.
     */
    public Uni<ArtifactMetaData> getArtifactMetadata(String serviceName) {
        String artifactId = serviceName + "-config";

        return Uni.createFrom().completionStage(() -> {
            CompletableFuture<ArtifactMetaData> future = new CompletableFuture<>();

            try {
                ArtifactMetaData metadata = registryClient.groups()
                        .byGroupId(DEFAULT_GROUP)
                        .artifacts()
                        .byArtifactId(artifactId)
                        .get();

                future.complete(metadata);
            } catch (Exception e) {
                LOG.debugf("Failed to get metadata for artifact %s: %s", artifactId, e.getMessage());
                ApicurioRegistryException exception = new ApicurioRegistryException(
                    String.format("Failed to get metadata for artifact %s", artifactId),
                    serviceName,
                    artifactId,
                    e
                );
                future.completeExceptionally(exception);
            }

            return future;
        });
    }

    public static class SchemaRegistrationResponse {
        private final String artifactId;
        private final Long globalId;
        private final String version;

        public SchemaRegistrationResponse(String artifactId, Long globalId, String version) {
            this.artifactId = artifactId;
            this.globalId = globalId;
            this.version = version;
        }

        public String getArtifactId() {
            return artifactId;
        }

        public Long getGlobalId() {
            return globalId;
        }

        public String getVersion() {
            return version;
        }
    }
}
