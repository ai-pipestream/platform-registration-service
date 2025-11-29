package ai.pipestream.registration.handlers;

import com.google.protobuf.Timestamp;
import ai.pipestream.data.module.MutinyPipeStepProcessorGrpc;
import ai.pipestream.data.module.RegistrationRequest;
import ai.pipestream.data.module.ServiceRegistrationMetadata;
import ai.pipestream.platform.registration.*;
import ai.pipestream.registration.consul.ConsulHealthChecker;
import ai.pipestream.registration.consul.ConsulRegistrar;
import ai.pipestream.registration.entity.ServiceModule;
import ai.pipestream.dynamic.grpc.client.DynamicGrpcClientFactory;
import ai.pipestream.registration.events.OpenSearchEventsProducer;
import ai.pipestream.registration.repository.ApicurioRegistryClient;
import ai.pipestream.registration.repository.ModuleRepository;
import io.smallrye.mutiny.Multi;
import io.smallrye.mutiny.Uni;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.smallrye.common.vertx.VertxContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * Handles module registration operations with proper reactive flow
 */
@ApplicationScoped
public class ModuleRegistrationHandler {
    
    private static final Logger LOG = Logger.getLogger(ModuleRegistrationHandler.class);
    
    @Inject
    ConsulRegistrar consulRegistrar;
    
    @Inject
    ConsulHealthChecker healthChecker;
    
    @Inject
    ModuleRepository moduleRepository;
    
    @Inject
    ApicurioRegistryClient apicurioClient;
    
    @Inject
    DynamicGrpcClientFactory grpcClientFactory;
    
    @Inject
    OpenSearchEventsProducer openSearchProducer;
    
    /**
     * Register a module with streaming status updates
     * Flow: Validate → Consul → Health → Fetch Metadata → Apicurio → Database → OpenSearch
     */
    public Multi<RegistrationEvent> registerModule(ModuleRegistrationRequest request) {
        String serviceId = ConsulRegistrar.generateServiceId(request.getModuleName(), request.getHost(), request.getPort());
        
        // Start with validation as a Uni
        return Uni.createFrom().item(() -> {
            if (!validateModuleRequest(request)) {
                throw new IllegalArgumentException("Invalid module registration request: Missing required fields");
            }
            return convertModuleToService(request);
        })
        .onItem().transformToMulti(serviceRequest -> {
            // Create a Multi that emits events throughout the registration process
            return Multi.createBy().concatenating()
                .streams(
                    Multi.createFrom().item(createEvent(EventType.STARTED, "Starting module registration", serviceId)),
                    Multi.createFrom().item(createEvent(EventType.VALIDATED, "Module registration request validated", null)),
                    executeModuleRegistrationAsMulti(request, serviceRequest, serviceId)
                );
        })
        .onFailure().recoverWithMulti(error -> {
            LOG.error("Module registration failed", error);
            return Multi.createFrom().items(
                createEventWithError(serviceId, "Registration failed", error.getMessage())
            );
        });
    }
    
    private Multi<RegistrationEvent> executeModuleRegistrationAsMulti(ModuleRegistrationRequest request, 
                                                                      ServiceRegistrationRequest serviceRequest,
                                                                      String serviceId) {
        return consulRegistrar.registerService(serviceRequest, serviceId)
            .onItem().transformToMulti(consulSuccess -> {
                if (!consulSuccess) {
                    return Multi.createFrom().item(
                        createEventWithError(serviceId, "Failed to register with Consul", "Consul registration failed")
                    );
                }
                
                return Multi.createBy().concatenating().streams(
                    Multi.createFrom().item(createEvent(EventType.CONSUL_REGISTERED, "Module registered with Consul", serviceId)),
                    Multi.createFrom().item(createEvent(EventType.HEALTH_CHECK_CONFIGURED, "Health check configured", null)),
                    continueRegistrationFlow(request, serviceId)
                );
            });
    }
    
    private Multi<RegistrationEvent> continueRegistrationFlow(ModuleRegistrationRequest request, String serviceId) {
        return healthChecker.waitForHealthy(request.getModuleName(), serviceId)
            .onItem().transformToMulti(healthy -> {
                if (!healthy) {
                    return rollbackConsulRegistration(serviceId)
                        .onItem().transformToMulti(v -> Multi.createFrom().item(
                            createEventWithError(serviceId, "Module failed health checks", 
                                "Module did not become healthy within timeout period")
                        ));
                }
                
                return Multi.createBy().concatenating().streams(
                    Multi.createFrom().item(createEvent(EventType.CONSUL_HEALTHY, "Module reported healthy by Consul", null)),
                    completeRegistrationFlow(request, serviceId)
                );
            });
    }
    
    private Multi<RegistrationEvent> completeRegistrationFlow(ModuleRegistrationRequest request, String serviceId) {
        return fetchModuleMetadata(request)
            .chain(metadata -> {
                String schema = extractOrSynthesizeSchema(metadata, request.getModuleName());
                Map<String, Object> metadataMap = buildMetadataMap(metadata);

                // Create a duplicated (safe) Vert.x context and switch the downstream onto it
                Context safeCtx = VertxContext.createNewDuplicatedContext();

                return Uni.createFrom().item(1)
                    .emitOn(r -> safeCtx.runOnContext(x -> r.run()))
                    .invoke(() -> {
                        Context ctx = Vertx.currentContext();
                        boolean duplicated = ctx != null && VertxContext.isDuplicatedContext(ctx);
                        LOG.debugf("DB segment context: present=%s, duplicated=%s, thread=%s",
                                ctx != null, duplicated, Thread.currentThread().getName());
                    })
                    .chain(ignored -> moduleRepository.registerModule(
                        request.getModuleName(),
                        request.getHost(),
                        request.getPort(),
                        request.getVersion(),
                        metadataMap,
                        schema
                    ))
                    .map(savedModule -> new DatabaseSaveContext(savedModule, metadata, schema));
            })
            // After database is done, we can call Apicurio on worker thread
            .chain(dbContext -> {
                return apicurioClient.createOrUpdateSchema(
                        request.getModuleName(),
                        request.getVersion(),
                        dbContext.schema
                    )
                    .map(schemaResult -> new SavedContext(dbContext.module, schemaResult))
                    .onFailure().recoverWithItem(err -> {
                        LOG.warnf(err, "Apicurio registration failed for %s:%s, continuing without registry sync",
                                request.getModuleName(), request.getVersion());
                        return new SavedContext(dbContext.module, null);
                    });
            })
            .onItem().transformToMulti(savedContext -> {
                // Emit to OpenSearch (fire and forget)
                openSearchProducer.emitModuleRegistered(
                    savedContext.module.serviceId,
                    request.getModuleName(),
                    request.getHost(),
                    request.getPort(),
                    request.getVersion(),
                    savedContext.module.configSchemaId,
                    savedContext.schemaResult != null ? savedContext.schemaResult.getArtifactId() : null
                );
                
                return Multi.createFrom().items(
                    createEvent(EventType.METADATA_RETRIEVED, "Module metadata retrieved", null),
                    createEvent(EventType.SCHEMA_VALIDATED, "Schema validated or synthesized", null),
                    createEvent(EventType.DATABASE_SAVED, "Module registration saved to database", savedContext.module.serviceId),
                    (savedContext.schemaResult != null
                        ? createEvent(EventType.APICURIO_REGISTERED, "Schema registered in Apicurio", null)
                        : createEvent(EventType.SCHEMA_VALIDATED, "Apicurio registry sync skipped (failure)", null)
                    ),
                    createEvent(EventType.COMPLETED, "Module registration completed successfully", savedContext.module.serviceId)
                );
            });
    }
    
    // Helper classes to pass context through the chain
    private static class DatabaseSaveContext {
        final ServiceModule module;
        final ServiceRegistrationMetadata metadata;
        final String schema;
        
        DatabaseSaveContext(ServiceModule module, ServiceRegistrationMetadata metadata, String schema) {
            this.module = module;
            this.metadata = metadata;
            this.schema = schema;
        }
    }
    
    private static class SavedContext {
        final ServiceModule module;
        final ApicurioRegistryClient.SchemaRegistrationResponse schemaResult;
        
        SavedContext(ServiceModule module, ApicurioRegistryClient.SchemaRegistrationResponse schemaResult) {
            this.module = module;
            this.schemaResult = schemaResult;
        }
    }
    
    
    /**
     * Unregister a module
     */
    public Uni<UnregisterResponse> unregisterModule(UnregisterRequest request) {
        String serviceId = ConsulRegistrar.generateServiceId(request.getServiceName(), request.getHost(), request.getPort());
        
        return consulRegistrar.unregisterService(serviceId)
            .map(success -> {
                UnregisterResponse.Builder response = UnregisterResponse.newBuilder()
                    .setSuccess(success)
                    .setTimestamp(createTimestamp());
                
                if (success) {
                    response.setMessage("Module unregistered successfully");
                    // Emit to OpenSearch
                    openSearchProducer.emitModuleUnregistered(serviceId, request.getServiceName());
                } else {
                    response.setMessage("Failed to unregister module");
                }
                
                return response.build();
            });
    }
    
    private Uni<ServiceRegistrationMetadata> fetchModuleMetadata(ModuleRegistrationRequest request) {
        String moduleName = request.getModuleName();
        return grpcClientFactory.getMutinyClientForService(moduleName)
            .onItem().transformToUni(stub -> 
                stub.getServiceRegistration(RegistrationRequest.newBuilder().build())
            );
    }
    
    private Uni<Void> rollbackConsulRegistration(String serviceId) {
        return consulRegistrar.unregisterService(serviceId)
            .onItem().invoke(success -> {
                if (success) {
                    LOG.infof("Rolled back Consul registration for %s", serviceId);
                } else {
                    LOG.errorf("Failed to rollback Consul registration for %s", serviceId);
                }
            })
            .replaceWith(Uni.createFrom().voidItem());
    }
    
    private Uni<Void> rollbackApicurioSchema(String moduleName, String version) {
        // TODO: Implement if Apicurio supports deletion
        LOG.warnf("Apicurio schema rollback not implemented for %s:%s", moduleName, version);
        return Uni.createFrom().voidItem();
    }
    
    private ServiceRegistrationRequest convertModuleToService(ModuleRegistrationRequest moduleRequest) {
        ServiceRegistrationRequest.Builder builder = ServiceRegistrationRequest.newBuilder()
            .setServiceName(moduleRequest.getModuleName())
            .setHost(moduleRequest.getHost())
            .setPort(moduleRequest.getPort())
            .setVersion(moduleRequest.getVersion())
            .putAllMetadata(moduleRequest.getMetadataMap())
            .addTags("module")
            .addTags("document-processor")
            .addCapabilities("PipeStepProcessor");
        
        // Add module metadata if present
        if (moduleRequest.hasServiceRegistrationMetadata()) {
            var metadata = moduleRequest.getServiceRegistrationMetadata();
            
            builder.putMetadata("module-name", metadata.getModuleName());
            builder.putMetadata("module-version", metadata.getVersion());
            
            if (metadata.hasJsonConfigSchema()) {
                builder.putMetadata("json-config-schema", metadata.getJsonConfigSchema());
            }
            if (metadata.hasDisplayName()) {
                builder.putMetadata("display-name", metadata.getDisplayName());
            }
            if (metadata.hasDescription()) {
                builder.putMetadata("description", metadata.getDescription());
            }
            
            builder.addAllTags(metadata.getTagsList());
        }
        
        return builder.build();
    }
    
    private String extractOrSynthesizeSchema(ServiceRegistrationMetadata metadata, String moduleName) {
        if (metadata.hasJsonConfigSchema() && !metadata.getJsonConfigSchema().isBlank()) {
            return metadata.getJsonConfigSchema();
        }
        
        // Synthesize a default key-value OpenAPI 3.1 schema
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
    
    private Map<String, Object> buildMetadataMap(ServiceRegistrationMetadata metadata) {
        Map<String, Object> map = new HashMap<>();
        map.putAll(metadata.getMetadataMap());
        
        if (metadata.hasDisplayName()) map.put("display_name", metadata.getDisplayName());
        if (metadata.hasDescription()) map.put("description", metadata.getDescription());
        if (metadata.hasOwner()) map.put("owner", metadata.getOwner());
        if (metadata.hasDocumentationUrl()) map.put("documentation_url", metadata.getDocumentationUrl());
        if (!metadata.getTagsList().isEmpty()) map.put("tags", metadata.getTagsList());
        if (!metadata.getDependenciesList().isEmpty()) map.put("dependencies", metadata.getDependenciesList());
        
        return map;
    }
    
    private boolean validateModuleRequest(ModuleRegistrationRequest request) {
        return !request.getModuleName().isEmpty() && 
               !request.getHost().isEmpty() && 
               request.getPort() > 0;
    }
    
    private RegistrationEvent createEvent(EventType type, String message, String serviceId) {
        RegistrationEvent.Builder builder = RegistrationEvent.newBuilder()
            .setEventType(type)
            .setMessage(message)
            .setTimestamp(createTimestamp());
        
        if (serviceId != null) {
            builder.setServiceId(serviceId);
        }
        
        return builder.build();
    }
    
    private RegistrationEvent createEventWithError(String serviceId, String message, String errorDetail) {
        RegistrationEvent.Builder builder = RegistrationEvent.newBuilder()
            .setEventType(EventType.FAILED)
            .setMessage(message)
            .setErrorDetail(errorDetail)
            .setTimestamp(createTimestamp());
        
        if (serviceId != null) {
            builder.setServiceId(serviceId);
        }
        
        return builder.build();
    }
    
    private Timestamp createTimestamp() {
        long millis = System.currentTimeMillis();
        return Timestamp.newBuilder()
            .setSeconds(millis / 1000)
            .setNanos((int) ((millis % 1000) * 1_000_000))
            .build();
    }
}
