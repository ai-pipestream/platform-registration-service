package ai.pipestream.registration.repository;

import ai.pipestream.registration.entity.ConfigSchema;
import ai.pipestream.registration.entity.ServiceModule;
import ai.pipestream.registration.entity.ServiceStatus;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.jboss.logging.Logger;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Repository for managing service module registrations in PostgreSQL.
 * This is the primary data store (system of record).
 *
 * <p>Blocking Hibernate ORM (Panache active-record) over JDBC. Write methods are
 * {@code @Transactional}; reads run without a transaction. Callers invoke these from
 * virtual threads, so the blocking JDBC work never pins the event loop.
 */
@ApplicationScoped
public class ModuleRepository {

    private static final Logger LOG = Logger.getLogger(ModuleRepository.class);

    @Inject
    ApicurioRegistryClient apicurioClient;

    /**
     * Register a new service module with optional schema.
     * Updates existing module if it already exists.
     */
    @Transactional
    public ServiceModule registerModule(String serviceName, String host, int port,
                                        String version, Map<String, Object> metadata,
                                        String jsonSchema) {
        String serviceId = ServiceModule.generateServiceId(serviceName, host, port);

        // First handle the schema if provided
        String schemaId = null;
        if (jsonSchema != null && !jsonSchema.isBlank()) {
            String candidateId = ConfigSchema.generateSchemaId(serviceName, version);
            ConfigSchema existingSchema = ConfigSchema.findById(candidateId);
            if (existingSchema != null) {
                // Same module@version re-registering with DIFFERENT content —
                // routine for SNAPSHOT versions during development (module
                // rebuilt, schema evolved). First-write-wins here meant a
                // module could never update its schema without a version
                // bump; the registry served the stale first registration
                // forever. Update in place and re-sync to Apicurio.
                if (!Objects.equals(existingSchema.jsonSchema, jsonSchema)) {
                    LOG.infof("Config schema content changed for %s@%s — updating", serviceName, version);
                    existingSchema.jsonSchema = jsonSchema;
                    existingSchema.syncStatus = ConfigSchema.SyncStatus.PENDING;
                }
                schemaId = existingSchema.schemaId;
            } else {
                ConfigSchema schema = ConfigSchema.create(serviceName, version, jsonSchema);
                schema.persist();
                schemaId = schema.schemaId;
            }
        }

        // Then handle the module
        ServiceModule existingModule = ServiceModule.findById(serviceId);
        if (existingModule != null) {
            boolean hasChanges = false;

            if (!Objects.equals(existingModule.version, version)) {
                existingModule.version = version;
                hasChanges = true;
            }
            if (!Objects.equals(existingModule.metadata, metadata)) {
                existingModule.metadata = metadata;
                hasChanges = true;
            }
            if (!Objects.equals(existingModule.configSchemaId, schemaId)) {
                existingModule.configSchemaId = schemaId;
                hasChanges = true;
            }

            // Always update heartbeat and status
            existingModule.updateHeartbeat();
            existingModule.status = ServiceStatus.ACTIVE;

            if (hasChanges) {
                LOG.infof("Updating existing module registration for %s", serviceId);
            } else {
                LOG.debugf("Module %s unchanged, only updating heartbeat", serviceId);
            }
            // Managed entity — dirty changes flush on transaction commit.
            return existingModule;
        }

        // Create new module
        ServiceModule module = ServiceModule.create(serviceName, host, port);
        module.version = version;
        module.metadata = metadata;
        module.configSchemaId = schemaId;
        LOG.infof("Creating new module registration for %s", serviceId);
        module.persist();
        return module;
    }

    /**
     * Save a configuration schema (dual storage: PostgreSQL + Apicurio).
     */
    @Transactional
    public ConfigSchema saveSchema(String serviceName, String version, String jsonSchema) {
        ConfigSchema schema = ConfigSchema.create(serviceName, version, jsonSchema);
        schema.persist();

        // Try to sync to Apicurio (best effort)
        try {
            syncSchemaToApicurio(schema);
        } catch (Exception error) {
            LOG.warnf("Failed to sync schema to Apicurio: %s", error.getMessage());
            schema.markSyncFailed(error.getMessage());
        }
        return schema;
    }

    /**
     * Sync schema to Apicurio Registry.
     */
    private void syncSchemaToApicurio(ConfigSchema schema) {
        ApicurioRegistryClient.SchemaRegistrationResponse response = apicurioClient.createOrUpdateSchema(
                schema.serviceName,
                schema.schemaVersion,
                schema.jsonSchema  // Already a String now
        );
        schema.markSynced(response.getArtifactId(), response.getGlobalId());
    }

    /**
     * Update heartbeat for a service.
     */
    @Transactional
    public ServiceModule updateHeartbeat(String serviceId) {
        ServiceModule module = ServiceModule.findById(serviceId);
        if (module != null) {
            module.updateHeartbeat();
            module.status = ServiceStatus.ACTIVE;
        }
        return module;
    }

    /**
     * Mark service as unhealthy.
     */
    @Transactional
    public ServiceModule markUnhealthy(String serviceId) {
        ServiceModule module = ServiceModule.findById(serviceId);
        if (module != null) {
            module.status = ServiceStatus.UNHEALTHY;
        }
        return module;
    }

    /**
     * Unregister a service.
     */
    @Transactional
    public boolean unregisterModule(String serviceId) {
        ServiceModule module = ServiceModule.findById(serviceId);
        if (module != null) {
            module.delete();
            return true;
        }
        return false;
    }

    /**
     * Get all active services.
     */
    public List<ServiceModule> getActiveServices() {
        return ServiceModule.list("status", ServiceStatus.ACTIVE);
    }

    /**
     * Get all services (for admin dashboard).
     */
    public List<ServiceModule> getAllServices() {
        return ServiceModule.listAll();
    }

    /**
     * Find stale services (no heartbeat for > 30 seconds).
     */
    public List<ServiceModule> findStaleServices() {
        LocalDateTime threshold = LocalDateTime.now().minusSeconds(30);
        return ServiceModule.list("status = ?1 and lastHeartbeat < ?2", ServiceStatus.ACTIVE, threshold);
    }

    /**
     * Get service by ID.
     */
    public ServiceModule findById(String serviceId) {
        return ServiceModule.findById(serviceId);
    }

    /**
     * Get schema by ID.
     */
    public ConfigSchema findSchemaById(String schemaId) {
        return ConfigSchema.findById(schemaId);
    }

    /**
     * Get latest schema for a service (most recently created).
     */
    /**
     * Upsert a NAMED schema artifact (recipe schema, uischema, ...) as a
     * ConfigSchema row keyed by the artifact name — same update-on-change
     * semantics as module config schemas, so SNAPSHOT re-registrations
     * refresh content and mark PENDING for Apicurio re-sync.
     */
    @Transactional
    public ConfigSchema upsertNamedArtifact(String artifactName, String version, String json) {
        String candidateId = ConfigSchema.generateSchemaId(artifactName, version);
        ConfigSchema existing = ConfigSchema.findById(candidateId);
        if (existing != null) {
            if (!Objects.equals(existing.jsonSchema, json)) {
                LOG.infof("Named artifact content changed for %s@%s — updating", artifactName, version);
                existing.jsonSchema = json;
                existing.syncStatus = ConfigSchema.SyncStatus.PENDING;
            }
            return existing;
        }
        ConfigSchema schema = ConfigSchema.create(artifactName, version, json);
        schema.persist();
        return schema;
    }

    public ConfigSchema findLatestSchemaByServiceName(String serviceName) {
        return ConfigSchema.find("serviceName = ?1 ORDER BY createdAt DESC", serviceName).firstResult();
    }

    /**
     * Resolve the schema of the module's CURRENT registration: the instance
     * with the freshest heartbeat wins. "Latest by createdAt" is wrong once
     * historical version rows exist (e.g. a branch-suffixed snapshot version
     * registered after the canonical one shadows it forever).
     */
    public ConfigSchema findCurrentSchemaByServiceName(String serviceName) {
        ServiceModule module = ServiceModule
                .find("serviceName = ?1 ORDER BY lastHeartbeat DESC", serviceName)
                .firstResult();
        if (module == null || module.configSchemaId == null) {
            return null;
        }
        return ConfigSchema.findById(module.configSchemaId);
    }

    /**
     * Get all schema versions for a service name, ordered by version descending.
     */
    public List<ConfigSchema> findSchemaVersionsByServiceName(String serviceName) {
        return ConfigSchema.list("serviceName = ?1 ORDER BY schemaVersion DESC", serviceName);
    }

    /**
     * Get all schemas needing sync to Apicurio.
     */
    public List<ConfigSchema> findSchemasNeedingSync() {
        return ConfigSchema.list("syncStatus in ?1",
                List.of(ConfigSchema.SyncStatus.PENDING,
                        ConfigSchema.SyncStatus.FAILED,
                        ConfigSchema.SyncStatus.OUT_OF_SYNC));
    }

    /**
     * Count registered services by status.
     */
    public Map<ServiceStatus, Long> countServicesByStatus() {
        Map<ServiceStatus, Long> counts = new HashMap<>();
        List<ServiceModule> services = ServiceModule.listAll();
        for (ServiceModule service : services) {
            counts.merge(service.status, 1L, Long::sum);
        }
        return counts;
    }
}
