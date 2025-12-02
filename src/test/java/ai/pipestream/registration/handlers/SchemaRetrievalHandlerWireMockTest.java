package ai.pipestream.registration.handlers;

import ai.pipestream.data.module.v1.PipeStepProcessorServiceGrpc;
import ai.pipestream.data.module.v1.GetServiceRegistrationResponse;
import ai.pipestream.platform.registration.v1.GetModuleSchemaRequest;
import ai.pipestream.platform.registration.v1.GetModuleSchemaResponse;
import ai.pipestream.registration.entity.ConfigSchema;
import ai.pipestream.registration.repository.ApicurioRegistryClient;
import ai.pipestream.registration.repository.ApicurioRegistryException;
import ai.pipestream.registration.repository.ModuleRepository;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.time.LocalDateTime;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * WireMock-based integration tests for SchemaRetrievalHandler.
 *
 * These tests are disabled until grpc-wiremock library is adapted for BSR protos.
 * The tests would validate the fallback mechanism to call modules directly via gRPC.
 */
@QuarkusTest
@Disabled("Disabled until grpc-wiremock library is adapted for BSR protos")
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
        // This test would verify that when database and Apicurio both fail,
        // the handler successfully falls back to calling the module directly via gRPC.
        // Currently disabled until grpc-wiremock is updated for BSR protos.
    }

    @Test
    void getModuleSchema_synthesizeSchema_whenModuleHasNoSchema() {
        // This test would verify that when a module returns metadata without a schema,
        // the handler synthesizes a default schema.
        // Currently disabled until grpc-wiremock is updated for BSR protos.
    }

    @Test
    void getModuleSchema_notFound_throwsException() {
        // This test would verify that when all sources fail (database, Apicurio, module),
        // a proper NOT_FOUND exception is thrown.
        // Currently disabled until grpc-wiremock is updated for BSR protos.
    }
}
