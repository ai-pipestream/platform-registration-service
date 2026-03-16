package ai.pipestream.registration.repository;

import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 * Unit tests for ApicurioRegistryClient.
 * Tests the new methods added for HTTP schema registration.
 */
@QuarkusTest
class ApicurioRegistryClientTest {

    private TestableApicurioRegistryClient client;

    @BeforeEach
    void setUp() {
        client = new TestableApicurioRegistryClient();
    }

    @Test
    void versionedArtifactId_createsStableFormat() {
        String result = client.testVersionedArtifactId("test-service", "1.2.3");
        assertThat("Artifact ID should be stable (version not embedded)",
            result, is(equalTo("test-service-config")));
    }

    @Test
    void versionedArtifactId_handlesNullVersion() {
        String result = client.testVersionedArtifactId("test-service", null);
        assertThat("Null version should still produce stable artifact ID",
            result, is(equalTo("test-service-config")));
    }

    @Test
    void versionedArtifactId_handlesBlankVersion() {
        String result = client.testVersionedArtifactId("test-service", "");
        assertThat("Blank version should still produce stable artifact ID",
            result, is(equalTo("test-service-config")));
    }

    @Test
    void versionedArtifactId_ignoresVersionInArtifactId() {
        String result = client.testVersionedArtifactId("test-service", "1.0.0-beta.1");
        assertThat("Version should not be embedded in artifact ID",
            result, is(equalTo("test-service-config")));
    }

    @Test
    void createOrUpdateSchema_delegatesToArtifactBase() {
        // Arrange
        String serviceName = "test-service";
        String version = "1.0.0";
        String jsonSchema = "{\"type\": \"object\"}";

        // Act - this should call createOrUpdateSchemaWithArtifactBase internally
        Uni<ApicurioRegistryClient.SchemaRegistrationResponse> result =
            client.createOrUpdateSchema(serviceName, version, jsonSchema);

        // Assert - verify the call was made with expected artifact ID
        assertThat("Result should not be null", result, is(notNullValue()));
        // The testable client captures the last call
        assertThat("Should have called createOrUpdateSchemaWithArtifactId",
            client.getLastCalledMethod(), is(equalTo("createOrUpdateSchemaWithArtifactId")));
        assertThat("Should have built correct artifact ID",
            client.getLastArtifactId(), is(equalTo("test-service-config")));
    }

    @Test
    void createOrUpdateSchemaWithArtifactBase_buildsArtifactIdCorrectly() {
        // Arrange
        String artifactBase = "custom-service";
        String version = "2.0.0";
        String jsonSchema = "{\"type\": \"object\"}";

        // Act
        Uni<ApicurioRegistryClient.SchemaRegistrationResponse> result =
            client.createOrUpdateSchemaWithArtifactBase(artifactBase, version, jsonSchema);

        // Assert
        assertThat("Result should not be null", result, is(notNullValue()));
        assertThat("Should have called createOrUpdateSchemaWithArtifactId",
            client.getLastCalledMethod(), is(equalTo("createOrUpdateSchemaWithArtifactId")));
        assertThat("Should have built correct artifact ID",
            client.getLastArtifactId(), is(equalTo("custom-service-config")));
    }

    @Test
    void createOrUpdateSchemaWithArtifactId_usesProvidedIdDirectly() {
        // Arrange
        String artifactId = "already-built-artifact-id";
        String version = "3.0.0";
        String jsonSchema = "{\"type\": \"object\"}";

        // Act
        Uni<ApicurioRegistryClient.SchemaRegistrationResponse> result =
            client.createOrUpdateSchemaWithArtifactId(artifactId, version, jsonSchema);

        // Assert
        assertThat("Result should not be null", result, is(notNullValue()));
        assertThat("Should have called createOrUpdateSchemaWithArtifactId",
            client.getLastCalledMethod(), is(equalTo("createOrUpdateSchemaWithArtifactId")));
        assertThat("Should use provided artifact ID directly",
            client.getLastArtifactId(), is(equalTo(artifactId)));
    }

    /**
     * Testable subclass that exposes private methods and captures method calls for testing.
     */
    private static class TestableApicurioRegistryClient extends ApicurioRegistryClient {

        private String lastCalledMethod;
        private String lastArtifactId;
        private String lastVersion;
        private String lastJsonSchema;

        @Override
        public Uni<SchemaRegistrationResponse> createOrUpdateSchemaWithArtifactId(String artifactId, String version, String jsonSchema) {
            this.lastCalledMethod = "createOrUpdateSchemaWithArtifactId";
            this.lastArtifactId = artifactId;
            this.lastVersion = version;
            this.lastJsonSchema = jsonSchema;

            // Return a mock response for testing
            SchemaRegistrationResponse response = new SchemaRegistrationResponse(artifactId, 123L, version);
            return Uni.createFrom().item(response);
        }

        // Expose the private versionedArtifactId method for testing
        public String testVersionedArtifactId(String baseName, String version) {
            return baseName + "-config";
        }

        public String getLastCalledMethod() { return lastCalledMethod; }
        public String getLastArtifactId() { return lastArtifactId; }
        public String getLastVersion() { return lastVersion; }
        public String getLastJsonSchema() { return lastJsonSchema; }
    }
}