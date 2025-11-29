package ai.pipestream.registration.handlers;

import ai.pipestream.data.module.PipeStepProcessorGrpc;
import ai.pipestream.grpc.wiremock.client.WireMockServerTestResource;
import ai.pipestream.platform.registration.GetModuleSchemaRequest;
import ai.pipestream.platform.registration.ModuleSchemaResponse;
import ai.pipestream.registration.entity.ConfigSchema;
import ai.pipestream.registration.repository.ApicurioRegistryClient;
import ai.pipestream.registration.repository.ApicurioRegistryException;
import ai.pipestream.registration.repository.ModuleRepository;
import com.github.tomakehurst.wiremock.client.WireMock;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
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
import org.mockito.Mockito;

import java.time.LocalDateTime;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static ai.pipestream.grpc.wiremock.client.WireMockGrpcClient.*;
import static com.github.tomakehurst.wiremock.client.WireMock.urlEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;

@QuarkusTest
@QuarkusTestResource(WireMockServerTestResource.class)
class SchemaRetrievalHandlerTest {

    @Inject
    SchemaRetrievalHandler schemaRetrievalHandler;

    @InjectMock
    ApicurioRegistryClient apicurioClient;

    @InjectMock
    ModuleRepository moduleRepository;

    @ConfigProperty(name = "wiremock.url")
    String wiremockUrl;

    @BeforeEach
    void setUp() {
        Mockito.reset(apicurioClient, moduleRepository);
        
        // Configure WireMock
        int httpPort = Integer.parseInt(wiremockUrl.substring(wiremockUrl.lastIndexOf(":") + 1));
        WireMock.configureFor("localhost", httpPort);
        WireMock.reset();
        
        // Initialize Stork to point services to WireMock
        if (Stork.getInstance() != null) {
            Stork.shutdown();
        }
        Stork.initialize(new DefaultStorkInfrastructure());
        
        // Point "test-module" to WireMock
        Map<String, String> params = Map.of("address-list", "localhost:" + httpPort);
        var discoveryConfig = new SimpleServiceConfig.SimpleServiceDiscoveryConfig("static", params);
        ServiceDefinition definition = ServiceDefinition.of(discoveryConfig);
        Stork.getInstance().defineIfAbsent("test-module", definition);
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
        ModuleSchemaResponse response = schemaRetrievalHandler.getModuleSchema(request)
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
        ModuleSchemaResponse response = schemaRetrievalHandler.getModuleSchema(request)
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
            .thenReturn(Uni.createFrom().failure(new ApicurioRegistryException("Metadata not found", serviceName, serviceName + "-config", null)));

        GetModuleSchemaRequest request = GetModuleSchemaRequest.newBuilder()
            .setModuleName(serviceName)
            .setVersion(version)
            .build();

        // Act
        ModuleSchemaResponse response = schemaRetrievalHandler.getModuleSchema(request)
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

    @Test
    void getModuleSchema_fallbackToModule_success() {
        // Arrange
        String serviceName = "test-module";
        String jsonSchema = "{\"type\": \"object\"}";

        when(moduleRepository.findLatestSchemaByServiceName(eq(serviceName)))
            .thenReturn(Uni.createFrom().nullItem());

        when(apicurioClient.getSchema(anyString(), anyString()))
            .thenReturn(Uni.createFrom().failure(new ApicurioRegistryException("Not found in Apicurio", serviceName, null, null)));
        
        // getArtifactMetadata won't be called if getSchema fails, but mock it just in case
        when(apicurioClient.getArtifactMetadata(anyString()))
            .thenReturn(Uni.createFrom().failure(new ApicurioRegistryException("Metadata not found", serviceName, null, null)));

        // Mock the gRPC service call using WireMock
        WireMock.stubFor(
            grpcStubFor(PipeStepProcessorGrpc.SERVICE_NAME, "GetServiceRegistration")
                .willReturn(aGrpcResponseWith(
                    ai.pipestream.data.module.ServiceRegistrationMetadata.newBuilder()
                        .setModuleName(serviceName)
                        .setVersion("1.0.0")
                        .setJsonConfigSchema(jsonSchema)
                        .setDisplayName("Test Module")
                        .setDescription("Test Description")
                        .build()
                ))
        );

        GetModuleSchemaRequest request = GetModuleSchemaRequest.newBuilder()
            .setModuleName(serviceName)
            .build();

        // Act
        ModuleSchemaResponse response = schemaRetrievalHandler.getModuleSchema(request)
            .await().indefinitely();

        // Assert
        assertThat("Response should not be null", response, is(notNullValue()));
        assertThat("Module name should match the requested service name",
            response.getModuleName(), is(equalTo(serviceName)));
        assertThat("Schema JSON should match the schema from module",
            response.getSchemaJson(), is(equalTo(jsonSchema)));
        assertThat("Schema version should match the module version",
            response.getSchemaVersion(), is(equalTo("1.0.0")));
        assertThat("Response should contain source metadata",
            response.containsMetadata("source"), is(true));
        assertThat("Source metadata should indicate module-direct retrieval",
            response.getMetadataOrThrow("source"), is(equalTo("module-direct")));
        assertThat("Response should contain display_name metadata",
            response.containsMetadata("display_name"), is(true));
        assertThat("Display name should match the module's display name",
            response.getMetadataOrThrow("display_name"), is(equalTo("Test Module")));
        assertThat("Response should contain description metadata",
            response.containsMetadata("description"), is(true));
        assertThat("Description should match the module's description",
            response.getMetadataOrThrow("description"), is(equalTo("Test Description")));
        
        // Verify that the gRPC call to the module was made
        WireMock.verify(postRequestedFor(urlEqualTo("/ai.pipestream.data.module.PipeStepProcessor/GetServiceRegistration")));
    }

    @Test
    void getModuleSchema_synthesizeSchema_whenModuleHasNoSchema() {
        // Arrange
        String serviceName = "test-module";

        when(moduleRepository.findLatestSchemaByServiceName(eq(serviceName)))
            .thenReturn(Uni.createFrom().nullItem());

        when(apicurioClient.getSchema(anyString(), anyString()))
            .thenReturn(Uni.createFrom().failure(new ApicurioRegistryException("Not found", serviceName, null, null)));

        // Mock the gRPC service call using WireMock (no schema provided)
        WireMock.stubFor(
            grpcStubFor(PipeStepProcessorGrpc.SERVICE_NAME, "GetServiceRegistration")
                .willReturn(aGrpcResponseWith(
                    ai.pipestream.data.module.ServiceRegistrationMetadata.newBuilder()
                        .setModuleName(serviceName)
                        .setVersion("1.0.0")
                        .build()
                ))
        );

        GetModuleSchemaRequest request = GetModuleSchemaRequest.newBuilder()
            .setModuleName(serviceName)
            .build();

        // Act
        ModuleSchemaResponse response = schemaRetrievalHandler.getModuleSchema(request)
            .await().indefinitely();

        // Assert
        assertThat("Response should not be null", response, is(notNullValue()));
        assertThat("Module name should match the requested service name",
            response.getModuleName(), is(equalTo(serviceName)));
        assertThat("Schema JSON should contain openapi specification",
            response.getSchemaJson(), containsString("openapi"));
        assertThat("Schema JSON should contain module configuration reference",
            response.getSchemaJson(), containsString(serviceName + " Configuration"));
        
        // Verify that the gRPC call to the module was made
        WireMock.verify(postRequestedFor(urlEqualTo("/ai.pipestream.data.module.PipeStepProcessor/GetServiceRegistration")));
    }

    @Test
    void getModuleSchema_notFound_throwsException() {
        // Arrange
        String serviceName = "non-existent-module";

        when(moduleRepository.findLatestSchemaByServiceName(eq(serviceName)))
            .thenReturn(Uni.createFrom().nullItem());

        when(apicurioClient.getSchema(anyString(), anyString()))
            .thenReturn(Uni.createFrom().failure(new ApicurioRegistryException("Not found", serviceName, null, null)));

        // Define the service in Stork but don't stub it in WireMock - this will cause the gRPC call to fail
        // which will be transformed into a StatusRuntimeException by the handler
        Map<String, String> params = Map.of("address-list", "localhost:9999"); // Invalid port
        var discoveryConfig = new SimpleServiceConfig.SimpleServiceDiscoveryConfig("static", params);
        ServiceDefinition definition = ServiceDefinition.of(discoveryConfig);
        Stork.getInstance().defineIfAbsent(serviceName, definition);

        GetModuleSchemaRequest request = GetModuleSchemaRequest.newBuilder()
            .setModuleName(serviceName)
            .build();

        // Act & Assert
        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () -> schemaRetrievalHandler.getModuleSchema(request).await().indefinitely());

        assertThat("Exception status code should be NOT_FOUND",
            exception.getStatus().getCode(), is(equalTo(Status.NOT_FOUND.getCode())));
        assertThat("Exception should have a description",
            exception.getStatus().getDescription(), is(notNullValue()));
        assertThat("Exception description should contain 'Module schema not found'",
            exception.getStatus().getDescription(), containsString("Module schema not found"));
    }
}
