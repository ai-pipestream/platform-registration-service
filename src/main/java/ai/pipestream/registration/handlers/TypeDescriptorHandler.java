package ai.pipestream.registration.handlers;

import ai.pipestream.platform.registration.v1.GetTypeDescriptorRequest;
import ai.pipestream.platform.registration.v1.GetTypeDescriptorResponse;
import ai.pipestream.platform.registration.v1.ListTypeDescriptorsRequest;
import ai.pipestream.platform.registration.v1.ListTypeDescriptorsResponse;
import ai.pipestream.platform.registration.v1.TypeDescriptorEntry;
import ai.pipestream.registration.descriptors.TypeDescriptorIndex;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

/**
 * Serves the descriptor door: resolves {@code Any} type_urls to self-contained
 * descriptor sets and lists the available type catalog, mapping
 * {@link TypeDescriptorIndex} results onto the documented gRPC error contract
 * (INVALID_ARGUMENT / NOT_FOUND / FAILED_PRECONDITION).
 */
@ApplicationScoped
public class TypeDescriptorHandler {

    @Inject
    TypeDescriptorIndex index;

    /**
     * Resolves one type to its self-contained, topologically ordered
     * {@code FileDescriptorSet}.
     *
     * @param request the type_url (or bare message name) and optional pinned version
     * @return the descriptor set, its content-hash version, and the resolved name
     * @throws StatusRuntimeException INVALID_ARGUMENT for a malformed type_url,
     *         NOT_FOUND for an unknown type, FAILED_PRECONDITION when an explicit
     *         version no longer matches the baked snapshot
     */
    public GetTypeDescriptorResponse getTypeDescriptor(GetTypeDescriptorRequest request) {
        String messageFullName = TypeDescriptorIndex.messageNameFromTypeUrl(request.getTypeUrl())
                .orElseThrow(() -> new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription(
                        "type_url '" + request.getTypeUrl() + "' is not a valid Any type_url or message full name")));

        TypeDescriptorIndex.TypeDescriptorPayload payload = index.lookup(messageFullName)
                .orElseThrow(() -> new StatusRuntimeException(Status.NOT_FOUND.withDescription(
                        "No message type '" + messageFullName + "' in the descriptor snapshot; "
                                + "use ListTypeDescriptors to browse available types")));

        if (request.hasVersion() && !request.getVersion().equals(payload.version())) {
            throw new StatusRuntimeException(Status.FAILED_PRECONDITION.withDescription(
                    "Requested version '" + request.getVersion() + "' for '" + messageFullName
                            + "' does not match the current snapshot version '" + payload.version()
                            + "'; historical descriptor versions are not retained"));
        }

        return GetTypeDescriptorResponse.newBuilder()
                .setFileDescriptorSet(ByteString.copyFrom(payload.fileDescriptorSet()))
                .setVersion(payload.version())
                .setMessageFullName(payload.messageFullName())
                .build();
    }

    /**
     * Lists every message type in the snapshot, optionally substring-filtered.
     *
     * @param request the optional case-insensitive substring filter
     * @return all matching types, sorted by full name
     */
    public ListTypeDescriptorsResponse listTypeDescriptors(ListTypeDescriptorsRequest request) {
        String filter = request.hasFilter() ? request.getFilter() : null;
        ListTypeDescriptorsResponse.Builder response = ListTypeDescriptorsResponse.newBuilder();
        for (TypeDescriptorIndex.TypeSummary summary : index.list(filter)) {
            response.addTypes(TypeDescriptorEntry.newBuilder()
                    .setMessageFullName(summary.messageFullName())
                    .setGroup(summary.group())
                    .setVersion(summary.version())
                    .build());
        }
        return response.build();
    }
}
