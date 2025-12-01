package ai.pipestream.registration.handlers;

import ai.pipestream.data.module.v1.PipeStepProcessorServiceGrpc;
import ai.pipestream.data.module.v1.GetServiceRegistrationResponse;
import ai.pipestream.platform.registration.v1.GetModuleSchemaRequest;
import ai.pipestream.platform.registration.v1.GetModuleSchemaResponse;
import ai.pipestream.registration.entity.ConfigSchema;
import ai.pipestream.registration.repository.ApicurioRegistryClient;
import ai.pipestream.registration.repository.ApicurioRegistryException;
import ai.pipestream.registration.repository.ModuleRepository;
import com.github.tomakehurst.wiremock.client.WireMock;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import io.smallrye.stork.Stork;
import io.smallrye.stork.api.ServiceDefinition;
import io.smallrye.stork.integration.DefaultStorkInfrastructure;
import io.smallrye.stork.spi.config.SimpleServiceConfig;
import jakarta.inject.Inject;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledIf;
import org.mockito.Mockito;

import java.time.LocalDateTime;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * Unit tests for SchemaRetrievalHandler.
 * Note: Tests using WireMock gRPC stubs are disabled until grpc-wiremock is adapted for BSR protos.
 */
@QuarkusTest
class SchemaRetrievalHandlerTest {

    @Inject
    SchemaRetrievalHandler schemaRetrievalHandler;

    @InjectMock
    ApicurioRegistryClient apicurioClient;

    @InjectMock
    ModuleRepository moduleRepository;

    @BeforeEach
    void setUp() {
        Mockito.reset(apicurioClient, moduleRepository);
    }

    @Test
    void getModuleSchema_fromDatabase_success() {
        // Arrange
        String serviceName = "test-module";
        String version = "1.0.0";
        String jsonSchema = "{\"type\": \"object\", \"properties\": {\"key\": {\"type\": \"string\"}}}";

        ConfigSchema schema = ConfigSchema.create(serviceName, version, jsonSchema);
        schema.createdBy = "test-user";
        schema.createdAt = LocalDateTime.now();
        schema.apicurioArtifactId = "test-artifact-id";
        schema.syncStatus = ConfigSchema.SyncStatus.SYNCED;

        when(moduleRepository.findSchemaById(anyString()))
            .thenReturn(Uni.createFrom().item(schema));

        GetModuleSchemaRequest request = GetModuleSchemaRequest.newBuilder()
            .setModuleName(serviceName)
            .setVersion(version)
            .build();

        // Act
        GetModuleSchemaResponse response = schemaRetrievalHandler.getModuleSchema(request)
            .await().indefinitely();

        // Assert
        assertThat("Response should not be null", response, is(notNullValue()));
        assertThat("Module name should match the requested service name",
            response.getModuleName(), is(equalTo(serviceName)));
        assertThat("Schema JSON should match the stored schema",
            response.getSchemaJson(), is(equalTo(jsonSchema)));
        assertThat("Schema version should match the requested version",
            response.getSchemaVersion(), is(equalTo(version)));
        assertThat("Artifact ID should match the stored artifact ID",
            response.getArtifactId(), is(equalTo("test-artifact-id")));
        assertThat("Response should contain created_by metadata",
            response.containsMetadata("created_by"), is(true));
        assertThat("Created by metadata should match the stored value",
            response.getMetadataOrThrow("created_by"), is(equalTo("test-user")));
        assertThat("Response should contain sync_status metadata",
            response.containsMetadata("sync_status"), is(true));
        assertThat("Sync status should be SYNCED",
            response.getMetadataOrThrow("sync_status"), is(equalTo("SYNCED")));
        assertThat("Response should have updatedAt timestamp",
            response.hasUpdatedAt(), is(true));

        verify(moduleRepository).findSchemaById(anyString());
        verifyNoInteractions(apicurioClient);
    }

    @Test
    void getModuleSchema_latestVersion_fromDatabase() {
        // Arrange
        String serviceName = "test-module";
        String jsonSchema = "{\"type\": \"object\"}";

        ConfigSchema schema = ConfigSchema.create(serviceName, "1.0.0", jsonSchema);
        schema.createdAt = LocalDateTime.now();

        when(moduleRepository.findLatestSchemaByServiceName(eq(serviceName)))
            .thenReturn(Uni.createFrom().item(schema));

        GetModuleSchemaRequest request = GetModuleSchemaRequest.newBuilder()
            .setModuleName(serviceName)
            .build(); // No version = latest

        // Act
        GetModuleSchemaResponse response = schemaRetrievalHandler.getModuleSchema(request)
            .await().indefinitely();

        // Assert
        assertThat("Response should not be null", response, is(notNullValue()));
        assertThat("Module name should match the requested service name",
            response.getModuleName(), is(equalTo(serviceName)));
        assertThat("Schema JSON should match the latest schema",
            response.getSchemaJson(), is(equalTo(jsonSchema)));

        verify(moduleRepository).findLatestSchemaByServiceName(eq(serviceName));
        verifyNoInteractions(apicurioClient);
    }

    @Test
    void getModuleSchema_fallbackToApicurio_success() {
        // Arrange
        String serviceName = "test-module";
        String version = "1.0.0";
        String jsonSchema = "{\"type\": \"object\", \"properties\": {\"key\": {\"type\": \"string\"}}}";

        when(moduleRepository.findSchemaById(anyString()))
            .thenReturn(Uni.createFrom().nullItem());

        when(apicurioClient.getSchema(eq(serviceName), eq(version)))
            .thenReturn(Uni.createFrom().item(jsonSchema));

        when(apicurioClient.getArtifactMetadata(eq(serviceName)))
            .thenReturn(Uni.createFrom().nullItem());

        GetModuleSchemaRequest request = GetModuleSchemaRequest.newBuilder()
            .setModuleName(serviceName)
            .setVersion(version)
            .build();

        // Act
        GetModuleSchemaResponse response = schemaRetrievalHandler.getModuleSchema(request)
            .await().indefinitely();

        // Assert
        assertThat("Response should not be null", response, is(notNullValue()));
        assertThat("Module name should match the requested service name",
            response.getModuleName(), is(equalTo(serviceName)));
        assertThat("Schema JSON should match the schema from Apicurio",
            response.getSchemaJson(), is(equalTo(jsonSchema)));
        assertThat("Schema version should match the requested version",
            response.getSchemaVersion(), is(equalTo(version)));

        verify(moduleRepository).findSchemaById(anyString());
        verify(apicurioClient).getSchema(eq(serviceName), eq(version));
        verify(apicurioClient).getArtifactMetadata(eq(serviceName));
    }

    // TODO: Re-enable this test once grpc-wiremock is adapted for BSR protos
    // @Test
    // void getModuleSchema_fallbackToModule_success() { ... }

    // TODO: Re-enable this test once grpc-wiremock is adapted for BSR protos
    // @Test
    // void getModuleSchema_synthesizeSchema_whenModuleHasNoSchema() { ... }

    // TODO: Re-enable this test once grpc-wiremock is adapted for BSR protos
    // @Test
    // void getModuleSchema_notFound_throwsException() { ... }
}
