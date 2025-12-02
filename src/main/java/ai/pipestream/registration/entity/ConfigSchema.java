package ai.pipestream.registration.entity;

import io.quarkus.hibernate.reactive.panache.PanacheEntityBase;
import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;

/**
 * Entity representing a configuration schema for a service.
 * This is the system of record, with Apicurio Registry as a secondary store.
 */
@Entity
@Table(name = "config_schemas")
public class ConfigSchema extends PanacheEntityBase {
    
    @Id
    @Column(name = "schema_id")
    public String schemaId;
    
    @Column(name = "service_name", nullable = false)
    public String serviceName;
    
    @Column(name = "schema_version", nullable = false)
    public String schemaVersion;
    
    @Column(name = "json_schema", nullable = false, columnDefinition = "JSON")
    @JdbcTypeCode(SqlTypes.JSON)
    public String jsonSchema;
    
    @CreationTimestamp
    @Column(name = "created_at")
    public LocalDateTime createdAt;
    
    @Column(name = "created_by")
    public String createdBy;
    
    @Column(name = "apicurio_artifact_id")
    public String apicurioArtifactId;
    
    @Column(name = "apicurio_global_id")
    public Long apicurioGlobalId;
    
    @Column(name = "sync_status")
    @Enumerated(EnumType.STRING)
    public SyncStatus syncStatus = SyncStatus.PENDING;
    
    @Column(name = "last_sync_attempt")
    public LocalDateTime lastSyncAttempt;
    
    @Column(name = "sync_error")
    public String syncError;
    
    /**
     * Factory method to create a new ConfigSchema.
     * @param serviceName the service name
     * @param version the schema version
     * @param jsonSchema the JSON schema string
     * @return the created ConfigSchema
     */
    public static ConfigSchema create(String serviceName, String version, String jsonSchema) {
        ConfigSchema schema = new ConfigSchema();
        schema.schemaId = generateSchemaId(serviceName, version);
        schema.serviceName = serviceName;
        schema.schemaVersion = version;
        schema.jsonSchema = jsonSchema;
        return schema;
    }
    
    /**
     * Generates a unique schema ID.
     * @param serviceName the service name
     * @param version the schema version
     * @return the generated schema ID
     */
    public static String generateSchemaId(String serviceName, String version) {
        return String.format("%s-v%s", serviceName, version.replace(".", "_"));
    }
    
    /**
     * Marks the schema as synced to Apicurio.
     * @param artifactId the Apicurio artifact ID
     * @param globalId the Apicurio global ID
     */
    public void markSynced(String artifactId, Long globalId) {
        this.apicurioArtifactId = artifactId;
        this.apicurioGlobalId = globalId;
        this.syncStatus = SyncStatus.SYNCED;
        this.lastSyncAttempt = LocalDateTime.now();
        this.syncError = null;
    }
    
    /**
     * Marks the schema sync as failed.
     * @param error the error message
     */
    public void markSyncFailed(String error) {
        this.syncStatus = SyncStatus.FAILED;
        this.lastSyncAttempt = LocalDateTime.now();
        this.syncError = error;
    }
    
    public enum SyncStatus {
        PENDING,    // Not yet synced to Apicurio
        SYNCED,     // Successfully synced
        FAILED,     // Sync failed
        OUT_OF_SYNC // Schema changed, needs re-sync
    }
}