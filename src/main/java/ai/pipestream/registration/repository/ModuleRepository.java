package ai.pipestream.registration.repository;

import ai.pipestream.registration.entity.ConfigSchema;
import ai.pipestream.registration.entity.ServiceModule;
import ai.pipestream.registration.entity.ServiceStatus;
import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.hibernate.reactive.mutiny.Mutiny;
import org.jboss.logging.Logger;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Repository for managing service module registrations in PostgreSQL.
 * This is the primary data store (system of record).
 */
@ApplicationScoped
public class ModuleRepository {
    
    private static final Logger LOG = Logger.getLogger(ModuleRepository.class);
    
    @Inject
    ApicurioRegistryClient apicurioClient;
    
    @Inject
    Mutiny.SessionFactory sessionFactory;
    
    /**
     * Register a new service module with optional schema  
     * Updates existing module if it already exists
     */
    public Uni<ServiceModule> registerModule(String serviceName, String host, int port, 
                                            String version, Map<String, Object> metadata,
                                            String jsonSchema) {
        String serviceId = ServiceModule.generateServiceId(serviceName, host, port);
        
        return sessionFactory.withTransaction(session -> {
            // First handle the schema if provided
            Uni<String> schemaIdUni;
            if (jsonSchema != null && !jsonSchema.isBlank()) {
                String schemaId = ConfigSchema.generateSchemaId(serviceName, version);
                schemaIdUni = session.find(ConfigSchema.class, schemaId)
                    .flatMap(existingSchema -> {
                        if (existingSchema != null) {
                            return Uni.createFrom().item(existingSchema.schemaId);
                        } else {
                            ConfigSchema schema = ConfigSchema.create(serviceName, version, jsonSchema);
                            return session.persist(schema).map(v -> schema.schemaId);
                        }
                    });
            } else {
                schemaIdUni = Uni.createFrom().nullItem();
            }
            
            // Then handle the module
            return schemaIdUni.flatMap(schemaId -> 
                session.find(ServiceModule.class, serviceId)
                    .flatMap(existingModule -> {
                        ServiceModule module;
                        if (existingModule != null) {
                            // Check if anything has actually changed
                            boolean hasChanges = false;
                            module = existingModule;
                            
                            if (!Objects.equals(module.version, version)) {
                                module.version = version;
                                hasChanges = true;
                            }
                            if (!Objects.equals(module.metadata, metadata)) {
                                module.metadata = metadata;
                                hasChanges = true;
                            }
                            if (!Objects.equals(module.configSchemaId, schemaId)) {
                                module.configSchemaId = schemaId;
                                hasChanges = true;
                            }
                            
                            // Always update heartbeat and status
                            module.updateHeartbeat();
                            module.status = ServiceStatus.ACTIVE;
                            
                            if (hasChanges) {
                                LOG.infof("Updating existing module registration for %s", serviceId);
                                return session.merge(module);
                            } else {
                                LOG.debugf("Module %s unchanged, only updating heartbeat", serviceId);
                                // Just update heartbeat - minimal update
                                return session.merge(module);
                            }
                        } else {
                            // Create new module
                            module = ServiceModule.create(serviceName, host, port);
                            module.version = version;
                            module.metadata = metadata;
                            module.configSchemaId = schemaId;
                            LOG.infof("Creating new module registration for %s", serviceId);
                            return session.persist(module).map(v -> module);
                        }
                    })
            );
        });
    }
    
    /**
     * Save a configuration schema (dual storage: PostgreSQL + Apicurio)
     */
    public Uni<ConfigSchema> saveSchema(String serviceName, String version, String jsonSchema) {
        ConfigSchema schema = ConfigSchema.create(serviceName, version, jsonSchema);
        
        return sessionFactory.withTransaction(session -> 
            session.persist(schema)
                .flatMap(v -> {
                    // Try to sync to Apicurio (best effort)
                    return syncSchemaToApicurio(schema)
                        .onFailure().invoke(error -> {
                            LOG.warnf("Failed to sync schema to Apicurio: %s", error.getMessage());
                            schema.markSyncFailed(error.getMessage());
                        })
                        .replaceWith(schema);
                })
        );
    }
    
    /**
     * Sync schema to Apicurio Registry
     */
    private Uni<ConfigSchema> syncSchemaToApicurio(ConfigSchema schema) {
        return apicurioClient.createOrUpdateSchema(
                schema.serviceName,
                schema.schemaVersion,
                schema.jsonSchema  // Already a String now
            )
            .map(response -> {
                schema.markSynced(response.getArtifactId(), response.getGlobalId());
                return schema;
            });
    }
    
    /**
     * Update heartbeat for a service
     */
    public Uni<ServiceModule> updateHeartbeat(String serviceId) {
        return sessionFactory.withTransaction(session ->
            session.find(ServiceModule.class, serviceId)
                .onItem().ifNotNull().invoke(module -> {
                    module.updateHeartbeat();
                    module.status = ServiceStatus.ACTIVE;
                })
        );
    }
    
    /**
     * Mark service as unhealthy
     */
    public Uni<ServiceModule> markUnhealthy(String serviceId) {
        return sessionFactory.withTransaction(session ->
            session.find(ServiceModule.class, serviceId)
                .onItem().ifNotNull().invoke(module -> {
                    module.status = ServiceStatus.UNHEALTHY;
                })
        );
    }
    
    /**
     * Unregister a service
     */
    public Uni<Boolean> unregisterModule(String serviceId) {
        return sessionFactory.withTransaction(session ->
            session.find(ServiceModule.class, serviceId)
                .onItem().ifNotNull().transformToUni(module -> 
                    session.remove(module).map(v -> true)
                )
                .onItem().ifNull().continueWith(false)
        );
    }
    
    /**
     * Get all active services
     */
    public Uni<List<ServiceModule>> getActiveServices() {
        return sessionFactory.withSession(session ->
            session.createQuery("FROM ServiceModule WHERE status = :status", ServiceModule.class)
                .setParameter("status", ServiceStatus.ACTIVE)
                .getResultList()
        );
    }
    
    /**
     * Get all services (for admin dashboard)
     */
    public Uni<List<ServiceModule>> getAllServices() {
        return sessionFactory.withSession(session ->
            session.createQuery("FROM ServiceModule", ServiceModule.class)
                .getResultList()
        );
    }
    
    /**
     * Find stale services (no heartbeat for > 30 seconds)
     */
    public Uni<List<ServiceModule>> findStaleServices() {
        LocalDateTime threshold = LocalDateTime.now().minusSeconds(30);
        return sessionFactory.withSession(session ->
            session.createQuery(
                "FROM ServiceModule WHERE status = :status AND lastHeartbeat < :threshold",
                ServiceModule.class
            )
            .setParameter("status", ServiceStatus.ACTIVE)
            .setParameter("threshold", threshold)
            .getResultList()
        );
    }
    
    /**
     * Get service by ID
     */
    public Uni<ServiceModule> findById(String serviceId) {
        return sessionFactory.withSession(session ->
            session.find(ServiceModule.class, serviceId)
        );
    }
    
    /**
     * Get schema by ID
     */
    public Uni<ConfigSchema> findSchemaById(String schemaId) {
        return sessionFactory.withSession(session ->
            session.find(ConfigSchema.class, schemaId)
        );
    }
    
    /**
     * Get latest schema for a service (most recently created)
     */
    public Uni<ConfigSchema> findLatestSchemaByServiceName(String serviceName) {
        return sessionFactory.withSession(session ->
            session.createQuery(
                "FROM ConfigSchema WHERE serviceName = :serviceName ORDER BY createdAt DESC",
                ConfigSchema.class
            )
            .setParameter("serviceName", serviceName)
            .setMaxResults(1)
            .getSingleResultOrNull()
        );
    }
    
    /**
     * Get all schema versions for a service name, ordered by version descending
     */
    public Uni<List<ConfigSchema>> findSchemaVersionsByServiceName(String serviceName) {
        return sessionFactory.withSession(session ->
            session.createQuery(
                "FROM ConfigSchema WHERE serviceName = :serviceName ORDER BY schemaVersion DESC",
                ConfigSchema.class
            )
            .setParameter("serviceName", serviceName)
            .getResultList()
        );
    }

    /**
     * Get all schemas needing sync to Apicurio
     */
    public Uni<List<ConfigSchema>> findSchemasNeedingSync() {
        return sessionFactory.withSession(session ->
            session.createQuery(
                "FROM ConfigSchema WHERE syncStatus IN ('PENDING', 'FAILED', 'OUT_OF_SYNC')",
                ConfigSchema.class
            ).getResultList()
        );
    }
    
    /**
     * Count registered services by status
     */
    public Uni<Map<ServiceStatus, Long>> countServicesByStatus() {
        return sessionFactory.withSession(session -> {
            Map<ServiceStatus, Long> counts = new java.util.HashMap<>();
            return session.createQuery("FROM ServiceModule", ServiceModule.class)
                .getResultList()
                .map(services -> {
                    for (ServiceModule service : services) {
                        counts.merge(service.status, 1L, Long::sum);
                    }
                    return counts;
                });
        });
    }
}