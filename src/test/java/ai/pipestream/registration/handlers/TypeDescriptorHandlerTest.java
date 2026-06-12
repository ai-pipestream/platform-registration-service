package ai.pipestream.registration.handlers;

import ai.pipestream.platform.registration.v1.GetTypeDescriptorRequest;
import ai.pipestream.platform.registration.v1.GetTypeDescriptorResponse;
import ai.pipestream.platform.registration.v1.ListTypeDescriptorsRequest;
import ai.pipestream.platform.registration.v1.ListTypeDescriptorsResponse;
import ai.pipestream.registration.descriptors.TypeDescriptorIndex;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the descriptor door's documented gRPC error contract:
 * INVALID_ARGUMENT (malformed type_url), NOT_FOUND (unknown type),
 * FAILED_PRECONDITION (stale pinned version), and the happy paths over the
 * real baked snapshot.
 */
class TypeDescriptorHandlerTest {

    private static final String TIKA_TYPE_URL = "type.googleapis.com/ai.pipestream.parsed.data.tika.v1.TikaResponse";

    private static TypeDescriptorHandler handler;

    @BeforeAll
    static void setUp() {
        TypeDescriptorIndex index = new TypeDescriptorIndex();
        index.load();
        handler = new TypeDescriptorHandler();
        handler.index = index;
    }

    @Test
    void resolvesTypeUrlToVersionedDescriptorSet() {
        GetTypeDescriptorResponse response = handler.getTypeDescriptor(
                GetTypeDescriptorRequest.newBuilder().setTypeUrl(TIKA_TYPE_URL).build());
        assertEquals("ai.pipestream.parsed.data.tika.v1.TikaResponse", response.getMessageFullName());
        assertFalse(response.getFileDescriptorSet().isEmpty());
        assertEquals(64, response.getVersion().length());
    }

    @Test
    void matchingPinnedVersionIsServed() {
        GetTypeDescriptorResponse first = handler.getTypeDescriptor(
                GetTypeDescriptorRequest.newBuilder().setTypeUrl(TIKA_TYPE_URL).build());
        GetTypeDescriptorResponse pinned = handler.getTypeDescriptor(
                GetTypeDescriptorRequest.newBuilder()
                        .setTypeUrl(TIKA_TYPE_URL)
                        .setVersion(first.getVersion())
                        .build());
        assertEquals(first.getVersion(), pinned.getVersion());
    }

    @Test
    void stalePinnedVersionIsFailedPrecondition() {
        StatusRuntimeException e = assertThrows(StatusRuntimeException.class, () ->
                handler.getTypeDescriptor(GetTypeDescriptorRequest.newBuilder()
                        .setTypeUrl(TIKA_TYPE_URL)
                        .setVersion("0".repeat(64))
                        .build()));
        assertEquals(Status.Code.FAILED_PRECONDITION, e.getStatus().getCode());
    }

    @Test
    void unknownTypeIsNotFound() {
        StatusRuntimeException e = assertThrows(StatusRuntimeException.class, () ->
                handler.getTypeDescriptor(GetTypeDescriptorRequest.newBuilder()
                        .setTypeUrl("type.googleapis.com/ai.pipestream.no.such.Type")
                        .build()));
        assertEquals(Status.Code.NOT_FOUND, e.getStatus().getCode());
    }

    @Test
    void malformedTypeUrlIsInvalidArgument() {
        StatusRuntimeException e = assertThrows(StatusRuntimeException.class, () ->
                handler.getTypeDescriptor(GetTypeDescriptorRequest.newBuilder()
                        .setTypeUrl("type.googleapis.com/")
                        .build()));
        assertEquals(Status.Code.INVALID_ARGUMENT, e.getStatus().getCode());
    }

    @Test
    void listReturnsFilteredCatalogWithGroupsAndVersions() {
        ListTypeDescriptorsResponse response = handler.listTypeDescriptors(
                ListTypeDescriptorsRequest.newBuilder().setFilter("TikaResponse").build());
        assertTrue(response.getTypesCount() >= 1);
        assertTrue(response.getTypesList().stream().anyMatch(t ->
                t.getMessageFullName().equals("ai.pipestream.parsed.data.tika.v1.TikaResponse")
                        && t.getGroup().equals("common")
                        && t.getVersion().length() == 64));
    }
}
