package ai.pipestream.registration.repository;

import io.apicurio.registry.rest.client.RegistryClient;
import io.apicurio.registry.rest.client.models.*;
import io.apicurio.registry.types.ArtifactType;
import io.apicurio.registry.utils.IoUtil;
import io.kiota.http.vertx.VertXRequestAdapter;
import io.vertx.core.Vertx;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Client for interacting with Apicurio Registry v3.
 * This is the secondary storage for schemas (primary is PostgreSQL).
 *
 * <p>The Apicurio v3 SDK calls are synchronous/blocking. Callers invoke these methods
 * from virtual threads, so the blocking I/O never pins the Vert.x event loop.
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
        this.requestAdapter = new VertXRequestAdapter(vertx);

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
     * Create or update a schema in Apicurio Registry.
     */
    public SchemaRegistrationResponse createOrUpdateSchema(String serviceName, String version, String jsonSchema) {
        String artifactId = versionedArtifactId(serviceName, version);
        return createOrUpdateSchemaWithArtifactId(artifactId, version, jsonSchema);
    }

    /**
     * Create or update a schema with a custom artifact base name.
     * The artifact ID will be built from the base name and version.
     */
    public SchemaRegistrationResponse createOrUpdateSchemaWithArtifactBase(String artifactBase, String version, String jsonSchema) {
        String artifactId = versionedArtifactId(artifactBase, version);
        return createOrUpdateSchemaWithArtifactId(artifactId, version, jsonSchema);
    }

    /**
     * Create or update a schema with a pre-built artifact ID.
     * The artifactId should already include the version suffix (use versionedArtifactId to build it).
     */
    public SchemaRegistrationResponse createOrUpdateSchemaWithArtifactId(String artifactId, String version, String jsonSchema) {
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
                        assert config.queryParameters != null;
                        config.queryParameters.ifExists = IfArtifactExists.FIND_OR_CREATE_VERSION;
                    })
                    .getVersion();

            // Create response
            assert versionMetaData != null;
            SchemaRegistrationResponse response = new SchemaRegistrationResponse(
                    artifactId,
                    versionMetaData.getGlobalId(),
                    versionMetaData.getVersion()
            );

            LOG.infof("Successfully registered schema with artifactId %s version %s (globalId: %d)",
                    artifactId, version, versionMetaData.getGlobalId());

            return response;
        } catch (Exception e) {
            String errorDetails = extractApicurioErrorDetails(e);

            // Version already exists is not an error — it means a prior registration succeeded
            if (errorDetails.contains("VersionAlreadyExists") || errorDetails.contains("already exists")) {
                LOG.infof("Schema already registered for artifactId=%s version=%s, reusing existing registration",
                        artifactId, version);
                return new SchemaRegistrationResponse(artifactId, null, version);
            }

            LOG.errorf(e, "Apicurio schema registration failed for artifactId=%s version=%s: %s",
                    artifactId, version, errorDetails);

            throw new ApicurioRegistryException(
                String.format("Failed to register schema for artifactId=%s version=%s: %s",
                        artifactId, version, errorDetails),
                null, // serviceName not available in this context
                artifactId,
                e
            );
        }
    }

    /**
     * Get schema by raw artifact ID.
     */
    public String getSchemaByArtifactId(String artifactId, String version) {
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

                assert content != null;
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

                assert content != null;
                return IoUtil.toString(content);
            }
        } catch (Exception e) {
            LOG.errorf(e, "Failed to get schema for artifactId %s", artifactId);
            throw new ApicurioRegistryException(
                String.format("Failed to get schema for artifactId %s (version: %s)", artifactId, version),
                null,
                artifactId,
                e
            );
        }
    }

    /** Raw content + content-type of an artifact in an ARBITRARY group (e.g. protobuf descriptors in "default"). */
    public record ArtifactContent(byte[] bytes, String contentType, String version) {}

    /**
     * Fetch any artifact's raw content from a specific group. Used by
     * GetSchemaArtifact for protobuf descriptor artifacts, which live
     * outside {@link #DEFAULT_GROUP} (the proto pipeline publishes them
     * under the descriptor group, conventionally "default").
     *
     * @return the content, or null when the artifact doesn't exist there
     */
    public ArtifactContent getArtifactContentFromGroup(String groupId, String artifactId, String version) {
        try {
            String expr = (version != null && !version.isEmpty()) ? version : "branch=latest";
            var meta = registryClient.groups().byGroupId(groupId)
                    .artifacts().byArtifactId(artifactId)
                    .versions().byVersionExpression(expr)
                    .get();
            var content = registryClient.groups().byGroupId(groupId)
                    .artifacts().byArtifactId(artifactId)
                    .versions().byVersionExpression(expr)
                    .content().get();
            if (content == null) {
                return null;
            }
            byte[] bytes = content.readAllBytes();
            String type = meta != null && "PROTOBUF".equalsIgnoreCase(meta.getArtifactType())
                    ? "application/x-protobuf" : "application/json";
            String servedVersion = meta != null && meta.getVersion() != null ? meta.getVersion() : "";
            return new ArtifactContent(bytes, type, servedVersion);
        } catch (Exception e) {
            LOG.debugf("Artifact %s not found in group %s: %s", artifactId, groupId, e.getMessage());
            return null;
        }
    }

    /**
     * Get schema by artifact ID (legacy naming, uses versionedArtifactId internally).
     */
    public String getSchema(String serviceName, String version) {
        String artifactId = versionedArtifactId(serviceName, version);
        return getSchemaByArtifactId(artifactId, version);
    }

    /**
     * Check if Apicurio is healthy.
     */
    public boolean isHealthy() {
        try {
            // Try to access the system info endpoint - THIS IS A BLOCKING CALL
            var systemInfo = registryClient.system().info().get();
            return systemInfo != null;
        } catch (Exception e) {
            LOG.debugf("Health check failed: %s", e.getMessage());
            return false;
        }
    }

    /**
     * List all artifacts in the group (for reconciliation).
     */
    public List<SearchedArtifact> listArtifacts() {
        try {
            // Search for all artifacts in our group
            ArtifactSearchResults results = registryClient.search()
                    .artifacts()
                    .get(config -> {
                        assert config.queryParameters != null;
                        config.queryParameters.groupId = DEFAULT_GROUP;
                        config.queryParameters.limit = 500;
                        config.queryParameters.offset = 0;
                    });

            assert results != null;
            return results.getArtifacts();
        } catch (Exception e) {
            LOG.errorf(e, "Failed to list artifacts");
            throw new ApicurioRegistryException("Failed to list artifacts", e);
        }
    }

    /**
     * Delete an artifact (for cleanup).
     */
    public boolean deleteArtifact(String serviceName) {
        String artifactId = serviceName + "-config";
        try {
            registryClient.groups()
                    .byGroupId(DEFAULT_GROUP)
                    .artifacts()
                    .byArtifactId(artifactId)
                    .delete();

            LOG.infof("Successfully deleted artifact %s", artifactId);
            return true;
        } catch (Exception e) {
            LOG.errorf(e, "Failed to delete artifact %s", artifactId);
            return false;
        }
    }

    private String versionedArtifactId(String baseName, String version) {
        // Artifact ID is stable by module name — version is tracked via Apicurio's version metadata,
        // not embedded in the artifact ID. FIND_OR_CREATE_VERSION handles deduplication.
        return baseName + "-config";
    }

    /**
     * Get artifact metadata.
     * Never returns null - throws ApicurioRegistryException if metadata cannot be retrieved.
     */
    public ArtifactMetaData getArtifactMetadata(String serviceName) {
        String artifactId = serviceName + "-config";
        try {
            return registryClient.groups()
                    .byGroupId(DEFAULT_GROUP)
                    .artifacts()
                    .byArtifactId(artifactId)
                    .get();
        } catch (Exception e) {
            LOG.debugf("Failed to get metadata for artifact %s: %s", artifactId, e.getMessage());
            throw new ApicurioRegistryException(
                String.format("Failed to get metadata for artifact %s", artifactId),
                serviceName,
                artifactId,
                e
            );
        }
    }

    /**
     * Extract detailed error information from Apicurio exceptions.
     */
    private String extractApicurioErrorDetails(Exception e) {
        try {
            // Check for RuleViolationProblemDetails
            if (e.getClass().getSimpleName().contains("RuleViolation")) {
                try {
                    var detailMethod = e.getClass().getMethod("getDetail");
                    var titleMethod = e.getClass().getMethod("getTitle");
                    var causeListMethod = e.getClass().getMethod("getCauses");

                    String detail = (String) detailMethod.invoke(e);
                    String title = (String) titleMethod.invoke(e);
                    @SuppressWarnings("unchecked")
                    var causes = (java.util.List<Object>) causeListMethod.invoke(e);

                    StringBuilder sb = new StringBuilder();
                    if (title != null) sb.append(title).append(": ");
                    if (detail != null) sb.append(detail);
                    if (causes != null && !causes.isEmpty()) {
                        sb.append(" (Causes: ");
                        for (int i = 0; i < causes.size(); i++) {
                            if (i > 0) sb.append(", ");
                            sb.append(causes.get(i).toString());
                        }
                        sb.append(")");
                    }
                    return sb.toString();
                } catch (Exception ignored) {
                    // Fall through to generic handling
                }
            }

            // Check for ApiException with response body
            Throwable current = e;
            while (current != null) {
                if (current.getClass().getSimpleName().contains("ApiException")) {
                    return "API error: " + current.getMessage();
                }
                current = current.getCause();
            }

            return e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
        } catch (Exception ex) {
            return "Unable to extract error details: " + e.getClass().getSimpleName();
        }
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
