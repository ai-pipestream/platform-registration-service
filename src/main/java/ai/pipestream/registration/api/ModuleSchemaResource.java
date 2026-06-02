package ai.pipestream.registration.api;

import ai.pipestream.platform.registration.v1.GetModuleSchemaRequest;
import ai.pipestream.platform.registration.v1.GetModuleSchemaResponse;
import ai.pipestream.registration.handlers.SchemaRetrievalHandler;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * REST facade for module JSON config schemas (JSONForms / graph editor).
 *
 * <p>Schemas are registered inline at module startup and stored in the
 * platform DB + Apicurio. This endpoint does not call running module instances.
 */
@Path("/modules")
public class ModuleSchemaResource {

    @Inject
    SchemaRetrievalHandler schemaRetrievalHandler;

    @GET
    @Path("/{name}/schema")
    @Produces(MediaType.APPLICATION_JSON)
    public Uni<Response> getModuleSchema(
            @PathParam("name") String name,
            @QueryParam("version") String version) {
        GetModuleSchemaRequest.Builder requestBuilder = GetModuleSchemaRequest.newBuilder()
                .setModuleName(name);
        if (version != null && !version.isBlank()) {
            requestBuilder.setVersion(version);
        }

        return schemaRetrievalHandler.getModuleSchema(requestBuilder.build())
                .map(this::toJsonResponse)
                .map(body -> Response.ok(body).build())
                .onFailure(StatusRuntimeException.class).transform(error -> {
                    StatusRuntimeException sre = (StatusRuntimeException) error;
                    if (sre.getStatus().getCode() == Status.Code.NOT_FOUND) {
                        throw new NotFoundException(sre.getStatus().getDescription(), sre);
                    }
                    throw error;
                });
    }

    private Map<String, Object> toJsonResponse(GetModuleSchemaResponse response) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("module_name", response.getModuleName());
        body.put("schema_version", response.getSchemaVersion());
        body.put("schema_json", response.getSchemaJson());
        if (!response.getArtifactId().isBlank()) {
            body.put("artifact_id", response.getArtifactId());
        }
        if (!response.getMetadataMap().isEmpty()) {
            body.put("metadata", response.getMetadataMap());
        }
        return body;
    }
}
