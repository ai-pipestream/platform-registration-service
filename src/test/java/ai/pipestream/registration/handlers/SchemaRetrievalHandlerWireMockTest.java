package ai.pipestream.registration.handlers;

import ai.pipestream.data.module.PipeStepProcessorGrpc;
import ai.pipestream.grpc.wiremock.client.ServiceMocks;
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

/**
 * Test for SchemaRetrievalHandler using WireMock for gRPC interactions.
 * This replaces the previous Mockito-based test for the gRPC client part,
 * which was prone to classloader issues.
 */
@QuarkusTest
@QuarkusTestResource(WireMockServerTestResource.class)
class SchemaRetrievalHandlerWireMockTest {

    @Inject
    SchemaRetrievalHandler schemaRetrievalHandler;

    // ApicurioRegistryClient is NOT mocked - the real one will be used against Dev Services
    @Inject
    ApicurioRegistryClient apicurioClient;

    @InjectMock
    ModuleRepository moduleRepository;

    @ConfigProperty(name = "wiremock.url")
    String wiremockUrl;

    private ServiceMocks mocks;

    @BeforeEach
    void setUp() {
        Mockito.reset(moduleRepository);
        
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
        
        mocks = new ServiceMocks();
    }

    @Test
    void getModuleSchema_fallbackToModule_success() {
        // Arrange
        String serviceName = "test-module";
        String jsonSchema = "{\"type\": \"object\"}";

        when(moduleRepository.findLatestSchemaByServiceName(eq(serviceName)))
            .thenReturn(Uni.createFrom().nullItem());

        // Real Apicurio client will be used. Since "test-module" doesn't exist in the registry,
        // it will throw ApicurioRegistryException, which will be caught by the handler's
        // onFailure(ApicurioRegistryException.class) handler, triggering the fallback to module.

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
        assertThat("Source metadata should indicate module-direct retrieval",
            response.getMetadataOrThrow("source"), is(equalTo("module-direct")));
        
        // Verify that the gRPC call to the module was made
        WireMock.verify(postRequestedFor(urlEqualTo("/ai.pipestream.data.module.PipeStepProcessor/GetServiceRegistration")));
    }

    @Test
    void getModuleSchema_synthesizeSchema_whenModuleHasNoSchema() {
        // Arrange
        String serviceName = "test-module";

        when(moduleRepository.findLatestSchemaByServiceName(eq(serviceName)))
            .thenReturn(Uni.createFrom().nullItem());

        // Real Apicurio client will throw ApicurioRegistryException (Not Found), 
        // which will be caught and trigger fallback to module.

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
        assertThat("Schema JSON should contain openapi specification",
            response.getSchemaJson(), containsString("openapi"));
        assertThat("Schema JSON should contain module configuration reference",
            response.getSchemaJson(), containsString(serviceName + " Configuration"));

        // Verify that the gRPC call to the module was made
        WireMock.verify(postRequestedFor(urlEqualTo("/ai.pipestream.data.module.PipeStepProcessor/GetServiceRegistration")));
    }

    @Test
    void getModuleSchema_fromDatabase_success() {
        // This test doesn't use gRPC but we include it to ensure full coverage in the WireMock suite
        String serviceName = "db-module";
        String version = "1.0.0";
        String jsonSchema = "{\"type\": \"object\"}";

        ConfigSchema schema = ConfigSchema.create(serviceName, version, jsonSchema);
        schema.createdBy = "test-user";
        schema.createdAt = LocalDateTime.now();
        schema.syncStatus = ConfigSchema.SyncStatus.SYNCED;

        when(moduleRepository.findSchemaById(anyString()))
            .thenReturn(Uni.createFrom().item(schema));

        GetModuleSchemaRequest request = GetModuleSchemaRequest.newBuilder()
            .setModuleName(serviceName)
            .setVersion(version)
            .build();

        ModuleSchemaResponse response = schemaRetrievalHandler.getModuleSchema(request)
            .await().indefinitely();

        assertThat(response.getSchemaJson(), is(equalTo(jsonSchema)));
        // No gRPC calls expected
    }
    
    @Test
    void getModuleSchema_notFound_throwsException() {
        // Arrange
        String serviceName = "non-existent-module";

        when(moduleRepository.findLatestSchemaByServiceName(eq(serviceName)))
            .thenReturn(Uni.createFrom().nullItem());

        // Real Apicurio client will throw ApicurioRegistryException.
        // Stork will fail to find the service if we don't define it, which is what happens here
        // since we only defined "test-module" in setUp(). This will cause the gRPC client factory
        // to fail, which will be transformed into a StatusRuntimeException by the handler.
        
        GetModuleSchemaRequest request = GetModuleSchemaRequest.newBuilder()
            .setModuleName(serviceName)
            .build();

        // Act & Assert
        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () ->
            schemaRetrievalHandler.getModuleSchema(request).await().indefinitely());

        assertThat(exception.getStatus().getCode(), is(equalTo(Status.NOT_FOUND.getCode())));
        assertThat(exception.getStatus().getDescription(), containsString("Module schema not found"));
    }
}
