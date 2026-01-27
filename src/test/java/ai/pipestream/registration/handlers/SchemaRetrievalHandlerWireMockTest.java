package ai.pipestream.registration.handlers;

import ai.pipestream.platform.registration.v1.GetModuleSchemaRequest;
import ai.pipestream.platform.registration.v1.GetModuleSchemaResponse;
import ai.pipestream.registration.repository.ApicurioRegistryClient;
import ai.pipestream.registration.repository.ApicurioRegistryException;
import ai.pipestream.registration.repository.ModuleRepository;
import io.grpc.StatusRuntimeException;
import io.quarkus.test.InjectMock;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.Duration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

/**
 * WireMock container-based integration tests for SchemaRetrievalHandler.
 * These validate fallback to module direct gRPC calls using pipestream-wiremock-server.
 */
@QuarkusTest
@QuarkusTestResource(ai.pipestream.registration.test.support.SchemaWireMockTestResource.class)
class SchemaRetrievalHandlerWireMockTest {

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
    void getModuleSchema_fallbackToModule_success() {
        String moduleName = "elasticsearch-sink";

        when(moduleRepository.findLatestSchemaByServiceName(eq(moduleName)))
            .thenReturn(Uni.createFrom().nullItem());

        when(apicurioClient.getSchema(eq(moduleName), anyString()))
            .thenReturn(Uni.createFrom().failure(
                new ApicurioRegistryException("Schema not found", moduleName, "artifact-id",
                    new RuntimeException("404 Not Found"))));

        GetModuleSchemaRequest request = GetModuleSchemaRequest.newBuilder()
            .setModuleName(moduleName)
            .build();

        GetModuleSchemaResponse response = schemaRetrievalHandler.getModuleSchema(request)
            .await().atMost(Duration.ofSeconds(10));

        assertThat("Response should not be null", response, is(notNullValue()));
        assertThat("Module name should match request",
            response.getModuleName(), is(equalTo(moduleName)));
        assertThat("Schema version should match module metadata",
            response.getSchemaVersion(), is(equalTo("1.0.0")));
        assertThat("Schema should be synthesized for missing module schema",
            response.getSchemaJson(), containsString("openapi"));
        assertThat("Schema should include module name",
            response.getSchemaJson(), containsString(moduleName + " Configuration"));
        assertThat("Source metadata should indicate module-direct",
            response.getMetadataOrThrow("source"), is(equalTo("module-direct")));
    }

    @Test
    void getModuleSchema_synthesizeSchema_whenModuleHasNoSchema() {
        String moduleName = "elasticsearch-sink";

        when(moduleRepository.findLatestSchemaByServiceName(eq(moduleName)))
            .thenReturn(Uni.createFrom().nullItem());

        when(apicurioClient.getSchema(eq(moduleName), anyString()))
            .thenReturn(Uni.createFrom().failure(
                new ApicurioRegistryException("Schema not found", moduleName, "artifact-id",
                    new RuntimeException("404 Not Found"))));

        GetModuleSchemaRequest request = GetModuleSchemaRequest.newBuilder()
            .setModuleName(moduleName)
            .build();

        GetModuleSchemaResponse response = schemaRetrievalHandler.getModuleSchema(request)
            .await().atMost(Duration.ofSeconds(10));

        assertThat("Schema should contain default Config schema",
            response.getSchemaJson(), containsString("\"Config\""));
        assertThat("Schema should allow additional properties",
            response.getSchemaJson(), containsString("\"additionalProperties\""));
    }

    @Test
    void getModuleSchema_notFound_throwsException() {
        String moduleName = "non-existent-module";

        when(moduleRepository.findLatestSchemaByServiceName(eq(moduleName)))
            .thenReturn(Uni.createFrom().nullItem());

        when(apicurioClient.getSchema(eq(moduleName), anyString()))
            .thenReturn(Uni.createFrom().failure(
                new ApicurioRegistryException("Schema not found", moduleName, "artifact-id",
                    new RuntimeException("404 Not Found"))));

        GetModuleSchemaRequest request = GetModuleSchemaRequest.newBuilder()
            .setModuleName(moduleName)
            .build();

        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () ->
            schemaRetrievalHandler.getModuleSchema(request)
                .await().atMost(Duration.ofSeconds(10))
        );

        assertThat("Exception status should be NOT_FOUND",
            exception.getStatus().getCode(), is(equalTo(io.grpc.Status.Code.NOT_FOUND)));
        assertThat("Exception message should include module name",
            exception.getMessage(), containsString(moduleName));
    }
}
