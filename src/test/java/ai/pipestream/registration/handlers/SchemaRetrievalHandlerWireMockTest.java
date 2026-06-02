package ai.pipestream.registration.handlers;

import ai.pipestream.platform.registration.v1.GetModuleSchemaRequest;
import ai.pipestream.registration.repository.ApicurioRegistryClient;
import ai.pipestream.registration.repository.ApicurioRegistryException;
import ai.pipestream.registration.repository.ModuleRepository;
import io.grpc.StatusRuntimeException;
import io.quarkus.test.InjectMock;
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
 * Schema retrieval uses DB then Apicurio only — no live module callback.
 */
@QuarkusTest
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
    void getModuleSchema_notFoundWhenDbAndApicurioMiss() {
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

        StatusRuntimeException exception = assertThrows(StatusRuntimeException.class, () ->
            schemaRetrievalHandler.getModuleSchema(request)
                .await().atMost(Duration.ofSeconds(10))
        );

        assertThat(exception.getStatus().getCode(), is(equalTo(io.grpc.Status.Code.NOT_FOUND)));
        assertThat(exception.getMessage(), containsString(moduleName));
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

        assertThat(exception.getStatus().getCode(), is(equalTo(io.grpc.Status.Code.NOT_FOUND)));
        assertThat(exception.getMessage(), containsString(moduleName));
    }
}
