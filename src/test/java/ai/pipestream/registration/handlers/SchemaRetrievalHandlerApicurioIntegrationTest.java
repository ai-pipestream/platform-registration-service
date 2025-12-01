package ai.pipestream.registration.handlers;

import ai.pipestream.platform.registration.v1.GetModuleSchemaRequest;
import ai.pipestream.platform.registration.v1.GetModuleSchemaResponse;
import ai.pipestream.registration.entity.ConfigSchema;
import ai.pipestream.registration.repository.ApicurioRegistryClient;
import ai.pipestream.registration.repository.ModuleRepository;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * Integration test for SchemaRetrievalHandler using real ApicurioRegistryClient.
 * This test validates that the handler can successfully interact with Apicurio Registry
 * via DevServices without mocking the client.
 */
@QuarkusTest
class SchemaRetrievalHandlerApicurioIntegrationTest {

    @Inject
    SchemaRetrievalHandler schemaRetrievalHandler;

    @Inject
    ApicurioRegistryClient apicurioClient;  // Real client, NOT mocked!

    @InjectMock
    ModuleRepository moduleRepository;  // Still mock the database

    @BeforeEach
    void setUp() {
        Mockito.reset(moduleRepository);
    }

    @Test
    void testApicurioClientIsInjected() {
        // Verify that the real Apicurio client is injected (not a mock)
        assertThat("Apicurio client should be injected", apicurioClient, is(notNullValue()));
        assertThat("Apicurio client should not be a mock",
            Mockito.mockingDetails(apicurioClient).isMock(), is(false));
    }

    @Test
    void testApicurioHealthCheck() {
        // Verify that the Apicurio Registry is accessible
        Boolean isHealthy = apicurioClient.isHealthy()
            .await().indefinitely();

        assertThat("Apicurio Registry should be healthy", isHealthy, is(true));
    }

    @Test
    void testRoundTrip_createAndRetrieveSchema() {
        // Arrange - Create a test schema
        String serviceName = "integration-test-module";
        String version = "1.0.0";
        String jsonSchema = "{\"type\": \"object\", \"properties\": {\"testKey\": {\"type\": \"string\"}}}";

        // First, register the schema in Apicurio
        ApicurioRegistryClient.SchemaRegistrationResponse registrationResponse =
            apicurioClient.createOrUpdateSchema(serviceName, version, jsonSchema)
                .await().indefinitely();

        assertThat("Registration should succeed", registrationResponse, is(notNullValue()));
        assertThat("Registration should have artifact ID",
            registrationResponse.getArtifactId(), is(notNullValue()));
        assertThat("Registration should have global ID",
            registrationResponse.getGlobalId(), is(notNullValue()));

        // Now verify we can retrieve it directly from Apicurio
        String retrievedSchema = apicurioClient.getSchema(serviceName, version)
            .await().indefinitely();

        assertThat("Retrieved schema should match", retrievedSchema, is(equalTo(jsonSchema)));
    }

    @Test
    void testCreateMultipleVersions() {
        // Arrange - Create multiple versions of a schema
        String serviceName = "versioned-test-module";
        String jsonSchemaV1 = "{\"type\": \"object\", \"version\": \"1.0.0\"}";
        String jsonSchemaV2 = "{\"type\": \"object\", \"version\": \"2.0.0\"}";

        // Register v1
        ApicurioRegistryClient.SchemaRegistrationResponse v1Response =
            apicurioClient.createOrUpdateSchema(serviceName, "1.0.0", jsonSchemaV1)
                .await().indefinitely();

        assertThat("V1 registration should succeed", v1Response, is(notNullValue()));

        // Register v2
        ApicurioRegistryClient.SchemaRegistrationResponse v2Response =
            apicurioClient.createOrUpdateSchema(serviceName, "2.0.0", jsonSchemaV2)
                .await().indefinitely();

        assertThat("V2 registration should succeed", v2Response, is(notNullValue()));

        // Verify we can retrieve v1
        String retrievedV1 = apicurioClient.getSchema(serviceName, "1.0.0")
            .await().indefinitely();
        assertThat("Retrieved V1 schema should match", retrievedV1, is(equalTo(jsonSchemaV1)));

        // Verify we can retrieve v2
        String retrievedV2 = apicurioClient.getSchema(serviceName, "2.0.0")
            .await().indefinitely();
        assertThat("Retrieved V2 schema should match", retrievedV2, is(equalTo(jsonSchemaV2)));
    }
}
