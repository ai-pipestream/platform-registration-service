package ai.pipestream.registration.handlers;

import ai.pipestream.platform.registration.v1.GetModuleSchemaRequest;
import ai.pipestream.platform.registration.v1.GetModuleSchemaVersionsRequest;
import ai.pipestream.platform.registration.v1.GetModuleSchemaResponse;
import ai.pipestream.platform.registration.v1.ModuleSchemaVersion;
import ai.pipestream.platform.registration.v1.GetModuleSchemaVersionsResponse;
import ai.pipestream.registration.entity.ConfigSchema;
import ai.pipestream.registration.repository.ApicurioRegistryClient;
import ai.pipestream.registration.repository.ApicurioRegistryException;
import ai.pipestream.registration.repository.ModuleRepository;
import com.google.protobuf.Timestamp;
import io.apicurio.registry.rest.client.models.ArtifactMetaData;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.time.Instant;
import java.time.ZoneOffset;

/**
 * Retrieves module configuration schemas from the platform registry (DB, then Apicurio).
 */
@ApplicationScoped
public class SchemaRetrievalHandler {

    private static final Logger LOG = Logger.getLogger(SchemaRetrievalHandler.class);

    @Inject
    ApicurioRegistryClient apicurioClient;

    @Inject
    ModuleRepository moduleRepository;

    public Uni<GetModuleSchemaResponse> getModuleSchema(GetModuleSchemaRequest request) {
        String serviceName = request.getModuleName();
        String version = request.hasVersion() && !request.getVersion().isEmpty()
            ? request.getVersion()
            : null;

        LOG.infof("Retrieving schema for module: %s, version: %s",
            serviceName, version != null ? version : "latest");

        return getSchemaFromDatabase(serviceName, version)
            .onItem().ifNotNull().transformToUni(this::buildResponseFromDatabase)
            .onItem().ifNull().switchTo(() -> {
                LOG.debugf("Schema not found in database for %s:%s, trying Apicurio",
                    serviceName, version);
                return getSchemaFromApicurio(serviceName, version);
            })
            .onFailure().transform(error -> {
                if (error instanceof StatusRuntimeException) {
                    return error;
                }
                if (error instanceof ApicurioRegistryException) {
                    return new StatusRuntimeException(
                            Status.NOT_FOUND.withDescription(
                                    "Module schema not found: " + serviceName).withCause(error));
                }
                LOG.errorf(error, "Unexpected error retrieving schema for %s:%s", serviceName, version);
                return new StatusRuntimeException(
                        Status.INTERNAL.withDescription(
                                "Failed to retrieve schema for " + serviceName).withCause(error));
            });
    }

    private Uni<ConfigSchema> getSchemaFromDatabase(String serviceName, String version) {
        if (version == null) {
            return moduleRepository.findLatestSchemaByServiceName(serviceName);
        }
        String schemaId = ConfigSchema.generateSchemaId(serviceName, version);
        return moduleRepository.findSchemaById(schemaId);
    }

    private Uni<GetModuleSchemaResponse> buildResponseFromDatabase(ConfigSchema schema) {
        GetModuleSchemaResponse.Builder builder = GetModuleSchemaResponse.newBuilder()
            .setModuleName(schema.serviceName)
            .setSchemaJson(schema.jsonSchema)
            .setSchemaVersion(schema.schemaVersion);

        if (schema.apicurioArtifactId != null) {
            builder.setArtifactId(schema.apicurioArtifactId);
        }

        if (schema.createdBy != null) {
            builder.putMetadata("created_by", schema.createdBy);
        }

        builder.putMetadata("sync_status", schema.syncStatus.name());

        if (schema.createdAt != null) {
            Instant instant = schema.createdAt.toInstant(ZoneOffset.UTC);
            builder.setUpdatedAt(Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build());
        }

        return Uni.createFrom().item(builder.build());
    }

    private Uni<GetModuleSchemaResponse> getSchemaFromApicurio(String serviceName, String version) {
        String versionToFetch = version != null ? version : "latest";

        return apicurioClient.getSchema(serviceName, versionToFetch)
            .flatMap(schemaContent -> apicurioClient.getArtifactMetadata(serviceName)
                    .map(metadata -> buildResponseFromApicurio(
                            serviceName, schemaContent, versionToFetch, metadata))
                    .onFailure(ApicurioRegistryException.class).recoverWithItem(error -> {
                        LOG.debugf(error, "Failed to get metadata for %s, using schema without metadata", serviceName);
                        return buildResponseFromApicurio(serviceName, schemaContent, versionToFetch, null);
                    }))
            .onFailure().transform(failure -> {
                if (failure instanceof ApicurioRegistryException) {
                    return new StatusRuntimeException(
                            Status.NOT_FOUND.withDescription(
                                    "Module schema not found: " + serviceName).withCause(failure));
                }
                return new ApicurioRegistryException(
                    String.format("Failed to get schema from Apicurio for %s", serviceName),
                    serviceName,
                    null,
                    failure
                );
            });
    }

    private GetModuleSchemaResponse buildResponseFromApicurio(
            String serviceName,
            String schemaContent,
            String version,
            ArtifactMetaData metadata) {

        GetModuleSchemaResponse.Builder builder = GetModuleSchemaResponse.newBuilder()
            .setModuleName(serviceName)
            .setSchemaJson(schemaContent)
            .setSchemaVersion(version);

        if (metadata != null) {
            if (metadata.getArtifactId() != null) {
                builder.setArtifactId(metadata.getArtifactId());
            }
            if (metadata.getOwner() != null) {
                builder.putMetadata("owner", metadata.getOwner());
            }
            if (metadata.getName() != null) {
                builder.putMetadata("name", metadata.getName());
            }
            if (metadata.getDescription() != null) {
                builder.putMetadata("description", metadata.getDescription());
            }
            if (metadata.getModifiedOn() != null) {
                Instant instant = Instant.ofEpochMilli(
                        metadata.getModifiedOn().toEpochSecond() * 1000
                                + metadata.getModifiedOn().getNano() / 1000000);
                builder.setUpdatedAt(Timestamp.newBuilder()
                    .setSeconds(instant.getEpochSecond())
                    .setNanos(instant.getNano())
                    .build());
            }
        }

        return builder.build();
    }

    public Uni<GetModuleSchemaVersionsResponse> getModuleSchemaVersions(GetModuleSchemaVersionsRequest request) {
        String serviceName = request.getModuleName();

        LOG.infof("Retrieving schema versions for module: %s", serviceName);

        return moduleRepository.findSchemaVersionsByServiceName(serviceName)
            .map(schemas -> {
                GetModuleSchemaVersionsResponse.Builder builder = GetModuleSchemaVersionsResponse.newBuilder()
                    .setModuleName(serviceName);

                String latestVersion = null;
                String artifactId = null;
                for (ConfigSchema schema : schemas) {
                    ModuleSchemaVersion.Builder versionBuilder = ModuleSchemaVersion.newBuilder()
                        .setVersion(schema.schemaVersion);

                    if (schema.createdAt != null) {
                        Instant instant = schema.createdAt.toInstant(ZoneOffset.UTC);
                        versionBuilder.setCreatedAt(Timestamp.newBuilder()
                            .setSeconds(instant.getEpochSecond())
                            .setNanos(instant.getNano())
                            .build());
                    }

                    if (schema.createdBy != null) {
                        versionBuilder.putMetadata("created_by", schema.createdBy);
                    }

                    builder.addVersions(versionBuilder.build());

                    if (latestVersion == null || schema.schemaVersion.compareTo(latestVersion) > 0) {
                        latestVersion = schema.schemaVersion;
                    }
                    if (schema.apicurioArtifactId != null) {
                        artifactId = schema.apicurioArtifactId;
                    }
                }

                if (artifactId != null) {
                    builder.setArtifactId(artifactId);
                }
                if (latestVersion != null) {
                    builder.setLatestVersion(latestVersion);
                }

                return builder.build();
            })
            .onItem().ifNull().switchTo(() -> Uni.createFrom().item(
                    GetModuleSchemaVersionsResponse.newBuilder()
                            .setModuleName(serviceName)
                            .build()));
    }
}
