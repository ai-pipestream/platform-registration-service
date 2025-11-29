package ai.pipestream.registration.handlers;

import ai.pipestream.data.module.RegistrationRequest;
import ai.pipestream.data.module.ServiceRegistrationMetadata;
import ai.pipestream.dynamic.grpc.client.DynamicGrpcClientFactory;
import ai.pipestream.platform.registration.GetModuleSchemaRequest;
import ai.pipestream.platform.registration.ModuleSchemaResponse;
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
 * Handles schema retrieval operations from Apicurio Registry with fallback to module direct calls
 */
@ApplicationScoped
public class SchemaRetrievalHandler {
    
    private static final Logger LOG = Logger.getLogger(SchemaRetrievalHandler.class);
    
    @Inject
    ApicurioRegistryClient apicurioClient;
    
    @Inject
    ModuleRepository moduleRepository;
    
    @Inject
    DynamicGrpcClientFactory grpcClientFactory;
    
    /**
     * Get module configuration schema from Apicurio Registry or fallback to module direct call
     */
    public Uni<ModuleSchemaResponse> getModuleSchema(GetModuleSchemaRequest request) {
        String serviceName = request.getModuleName();
        String version = request.hasVersion() && !request.getVersion().isEmpty() 
            ? request.getVersion() 
            : null;
        
        LOG.infof("Retrieving schema for module: %s, version: %s", 
            serviceName, version != null ? version : "latest");
        
        // Try to get schema from database first (system of record)
        return getSchemaFromDatabase(serviceName, version)
            .onItem().ifNotNull().transformToUni(this::buildResponseFromDatabase)
            .onItem().ifNull().switchTo(() -> {
                LOG.debugf("Schema not found in database for %s:%s, trying Apicurio", 
                    serviceName, version);
                return getSchemaFromApicurio(serviceName, version)
                    .onFailure().recoverWithUni(error -> {
                        // Check if it's an ApicurioRegistryException or if the cause is one
                        if (error instanceof ApicurioRegistryException || 
                            (error.getCause() != null && error.getCause() instanceof ApicurioRegistryException)) {
                            LOG.warnf(error, "Failed to get schema from Apicurio for %s:%s, falling back to module", 
                                serviceName, version);
                            return getSchemaFromModule(serviceName)
                                .onFailure().transform(moduleError -> {
                                    LOG.errorf(moduleError, "Module fallback failed for %s (error type: %s), transforming to StatusRuntimeException", 
                                        serviceName, moduleError.getClass().getName());
                                    if (moduleError instanceof StatusRuntimeException) {
                                        return moduleError;
                                    }
                                    return new StatusRuntimeException(
                                        Status.NOT_FOUND.withDescription(
                                            "Module schema not found: " + serviceName + 
                                            ". Module may not be running or registered."
                                        ).withCause(moduleError)
                                    );
                                });
                        }
                        // For other errors, also try module fallback
                        LOG.warnf(error, "Unexpected error from Apicurio for %s:%s, trying module fallback", 
                            serviceName, version);
                        return getSchemaFromModule(serviceName)
                            .onFailure().transform(moduleError -> {
                                LOG.errorf(moduleError, "Module fallback failed for %s (error type: %s), transforming to StatusRuntimeException", 
                                    serviceName, moduleError.getClass().getName());
                                if (moduleError instanceof StatusRuntimeException) {
                                    return moduleError;
                                }
                                return new StatusRuntimeException(
                                    Status.NOT_FOUND.withDescription(
                                        "Module schema not found: " + serviceName + 
                                        ". Module may not be running or registered."
                                    ).withCause(moduleError)
                                );
                            });
                    });
            })
            .onFailure().recoverWithUni(error -> {
                // Check if it's a StatusRuntimeException (from module fallback) or if it's wrapped
                Throwable cause = error;
                while (cause != null && !(cause instanceof StatusRuntimeException)) {
                    if (cause.getCause() != null && cause.getCause() != cause) {
                        cause = cause.getCause();
                    } else {
                        break;
                    }
                }
                
                if (cause instanceof StatusRuntimeException) {
                    // Propagate StatusRuntimeException from module fallback (e.g., NOT_FOUND)
                    return Uni.createFrom().failure(cause);
                }
                
                // Catch any other unexpected failures (from database lookup - shouldn't happen)
                LOG.errorf(error, "Unexpected error retrieving schema for %s:%s", serviceName, version);
                return Uni.createFrom().failure(error);
            });
    }
    
    /**
     * Get schema from database (system of record)
     */
    private Uni<ConfigSchema> getSchemaFromDatabase(String serviceName, String version) {
        if (version == null) {
            // Get latest version by service name
            return moduleRepository.findLatestSchemaByServiceName(serviceName);
        } else {
            String schemaId = ConfigSchema.generateSchemaId(serviceName, version);
            return moduleRepository.findSchemaById(schemaId);
        }
    }
    
    /**
     * Build response from database schema
     */
    private Uni<ModuleSchemaResponse> buildResponseFromDatabase(ConfigSchema schema) {
        ModuleSchemaResponse.Builder builder = ModuleSchemaResponse.newBuilder()
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
    
    /**
     * Get schema from Apicurio Registry
     */
    private Uni<ModuleSchemaResponse> getSchemaFromApicurio(String serviceName, String version) {
        String versionToFetch = version != null ? version : "latest";
        
        return apicurioClient.getSchema(serviceName, versionToFetch)
            .flatMap(schemaContent -> {
                // Get artifact metadata for additional information
                // If metadata retrieval fails, we still have the schema content, so we can continue
                return apicurioClient.getArtifactMetadata(serviceName)
                    .map(metadata -> buildResponseFromApicurio(
                        serviceName, 
                        schemaContent, 
                        versionToFetch,
                        metadata
                    ))
                    .onFailure(ApicurioRegistryException.class).recoverWithItem(error -> {
                        // If metadata fails, build response without metadata
                        LOG.debugf(error, "Failed to get metadata for %s, using schema without metadata", serviceName);
                        return buildResponseFromApicurio(serviceName, schemaContent, versionToFetch, null);
                    });
            })
            .onFailure().transform(failure -> {
                // Ensure ApicurioRegistryException is properly propagated
                if (failure instanceof ApicurioRegistryException) {
                    return failure;
                }
                // Wrap unexpected failures
                return new ApicurioRegistryException(
                    String.format("Failed to get schema from Apicurio for %s", serviceName),
                    serviceName,
                    null,
                    failure
                );
            });
    }
    
    /**
     * Build response from Apicurio artifact
     */
    private ModuleSchemaResponse buildResponseFromApicurio(
            String serviceName,
            String schemaContent, 
            String version,
            ArtifactMetaData metadata) {
        
        ModuleSchemaResponse.Builder builder = ModuleSchemaResponse.newBuilder()
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
                Instant instant = Instant.ofEpochMilli(metadata.getModifiedOn().toEpochSecond() * 1000 + metadata.getModifiedOn().getNano() / 1000000) ;
                builder.setUpdatedAt(Timestamp.newBuilder()
                    .setSeconds(instant.getEpochSecond())
                    .setNanos(instant.getNano())
                    .build());
            }
        }
        
        return builder.build();
    }
    
    /**
     * Fallback: Get schema by calling module's getServiceRegistration() directly
     */
    private Uni<ModuleSchemaResponse> getSchemaFromModule(String serviceName) {
        LOG.infof("Falling back to direct module call for schema: %s", serviceName);
        
        return grpcClientFactory.getMutinyClientForService(serviceName)
            .flatMap(stub -> 
                stub.getServiceRegistration(RegistrationRequest.newBuilder().build())
            )
            .map(metadata -> buildResponseFromModuleMetadata(serviceName, metadata))
            .onFailure().transform(error -> {
                LOG.errorf(error, "Failed to get schema from module %s (error type: %s)", 
                    serviceName, error.getClass().getName());
                // Preserve StatusRuntimeException with NOT_FOUND status
                if (error instanceof StatusRuntimeException) {
                    StatusRuntimeException sre = (StatusRuntimeException) error;
                    // If it's already NOT_FOUND, preserve it
                    if (sre.getStatus().getCode() == Status.NOT_FOUND.getCode()) {
                        return error;
                    }
                    // Otherwise, transform to NOT_FOUND (e.g., UNIMPLEMENTED from WireMock means service not found)
                    return new StatusRuntimeException(
                        Status.NOT_FOUND.withDescription(
                            "Module schema not found: " + serviceName + 
                            ". Module may not be running or registered."
                        ).withCause(error)
                    );
                }
                // Transform other errors to StatusRuntimeException
                return new StatusRuntimeException(
                    Status.NOT_FOUND.withDescription(
                        "Module schema not found: " + serviceName + 
                        ". Module may not be running or registered."
                    ).withCause(error)
                );
            });
    }
    
    /**
     * Build response from module's ServiceRegistrationMetadata
     */
    private ModuleSchemaResponse buildResponseFromModuleMetadata(
            String serviceName,
            ServiceRegistrationMetadata metadata) {
        
        ModuleSchemaResponse.Builder builder = ModuleSchemaResponse.newBuilder()
            .setModuleName(serviceName);
        
        if (metadata.hasJsonConfigSchema() && !metadata.getJsonConfigSchema().isBlank()) {
            builder.setSchemaJson(metadata.getJsonConfigSchema());
        } else {
            // Return a minimal schema if none provided
            builder.setSchemaJson(synthesizeDefaultSchema(serviceName));
        }
        
        if (!metadata.getVersion().isBlank()) {
            builder.setSchemaVersion(metadata.getVersion());
        } else {
            builder.setSchemaVersion("unknown");
        }
        
        builder.putMetadata("source", "module-direct");
        
        if (metadata.hasDisplayName()) {
            builder.putMetadata("display_name", metadata.getDisplayName());
        }
        
        if (metadata.hasDescription()) {
            builder.putMetadata("description", metadata.getDescription());
        }
        
        if (metadata.hasOwner()) {
            builder.putMetadata("owner", metadata.getOwner());
        }
        
        // Set current timestamp as updated_at
        Instant now = Instant.now();
        builder.setUpdatedAt(Timestamp.newBuilder()
            .setSeconds(now.getEpochSecond())
            .setNanos(now.getNano())
            .build());
        
        return builder.build();
    }
    
    /**
     * Synthesize a default schema if module doesn't provide one
     */
    private String synthesizeDefaultSchema(String moduleName) {
        return "{\n" +
            "  \"openapi\": \"3.1.0\",\n" +
            "  \"info\": { \"title\": \"" + moduleName + " Configuration\", \"version\": \"1.0.0\" },\n" +
            "  \"components\": {\n" +
            "    \"schemas\": {\n" +
            "      \"Config\": {\n" +
            "        \"type\": \"object\",\n" +
            "        \"additionalProperties\": { \"type\": \"string\" },\n" +
            "        \"description\": \"Key-value configuration for " + moduleName + "\"\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
    }
}
