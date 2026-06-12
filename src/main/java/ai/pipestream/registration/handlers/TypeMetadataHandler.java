package ai.pipestream.registration.handlers;

import ai.pipestream.platform.registration.v1.GetTypeMetadataRequest;
import ai.pipestream.platform.registration.v1.GetTypeMetadataResponse;
import ai.pipestream.platform.registration.v1.PromoteTypeMetadataRequest;
import ai.pipestream.platform.registration.v1.PromoteTypeMetadataResponse;
import ai.pipestream.platform.registration.v1.SaveTypeMetadataRequest;
import ai.pipestream.platform.registration.v1.SaveTypeMetadataResponse;
import ai.pipestream.registration.metadata.TypeMetadataService;
import com.google.protobuf.Timestamp;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.time.Instant;
import java.util.NoSuchElementException;

/**
 * Serves the type metadata catalog doors, mapping
 * {@link TypeMetadataService} results onto the documented gRPC error
 * contract (NOT_FOUND unknown type / missing draft; INVALID_ARGUMENT bad
 * scope combination or unknown field_path).
 */
@ApplicationScoped
public class TypeMetadataHandler {

    @Inject
    TypeMetadataService metadataService;

    /**
     * One type's merged metadata view.
     *
     * @param request the type and optional graph scope
     * @return the merged view with per-layer provenance
     */
    public GetTypeMetadataResponse getTypeMetadata(GetTypeMetadataRequest request) {
        try {
            return metadataService.getMerged(
                    request.getMessageFullName(),
                    request.hasGraphId() ? request.getGraphId() : null);
        } catch (NoSuchElementException e) {
            throw new StatusRuntimeException(Status.NOT_FOUND.withDescription(e.getMessage()));
        }
    }

    /**
     * Saves one scope's annotations.
     *
     * @param request the scope, type and full-replacement fields
     * @return the persisted state
     */
    public SaveTypeMetadataResponse saveTypeMetadata(SaveTypeMetadataRequest request) {
        try {
            TypeMetadataService.SaveResult saved = metadataService.save(
                    request.getScope(), request.getGraphId(),
                    request.getMessageFullName(), request.getFieldsMap());
            return SaveTypeMetadataResponse.newBuilder()
                    .setMessageFullName(request.getMessageFullName())
                    .setFieldCount(saved.fieldCount())
                    .setUpdatedAt(toTimestamp(saved.updatedAt()))
                    .build();
        } catch (NoSuchElementException e) {
            throw new StatusRuntimeException(Status.NOT_FOUND.withDescription(e.getMessage()));
        } catch (IllegalArgumentException e) {
            throw new StatusRuntimeException(Status.INVALID_ARGUMENT.withDescription(e.getMessage()));
        }
    }

    /**
     * Promotes a type's global draft (phase 1).
     *
     * @param request the type
     * @return the Apicurio version + pending-bake id
     */
    public PromoteTypeMetadataResponse promoteTypeMetadata(PromoteTypeMetadataRequest request) {
        try {
            TypeMetadataService.PromoteResult promoted =
                    metadataService.promote(request.getMessageFullName());
            return PromoteTypeMetadataResponse.newBuilder()
                    .setMessageFullName(request.getMessageFullName())
                    .setApicurioVersion(promoted.apicurioVersion())
                    .setPendingBakeId(promoted.pendingBakeId())
                    .build();
        } catch (NoSuchElementException e) {
            throw new StatusRuntimeException(Status.NOT_FOUND.withDescription(e.getMessage()));
        }
    }

    private static Timestamp toTimestamp(Instant instant) {
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }
}
