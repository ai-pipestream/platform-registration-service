package ai.pipestream.registration.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.Instant;

/**
 * One promotion awaiting the phase-2 git bake: a snapshot of the annotations
 * that were written to the Apicurio system of record, queued for the (future)
 * automation that turns them into a protos PR so the next build bakes them
 * into descriptors.
 */
@Entity
@Table(name = "metadata_pending_bakes")
public class MetadataPendingBake extends PanacheEntityBase {

    /** Creates an empty entity instance for JPA. */
    public MetadataPendingBake() {
    }

    /** Bake record id (UUID string). */
    @Id
    @Column(name = "id", nullable = false, length = 36)
    public String id;

    /** The promoted type. */
    @Column(name = "message_full_name", nullable = false, length = 512)
    public String messageFullName;

    /** Apicurio overlay version written by the promotion. */
    @Column(name = "apicurio_version", nullable = false, length = 64)
    public String apicurioVersion;

    /** JSON envelope of the promoted field annotations. */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "fields", nullable = false, columnDefinition = "JSONB")
    public String fieldsJson;

    /** Bake status; PENDING until phase-2 automation consumes it. */
    @Column(name = "status", nullable = false, length = 32)
    public String status;

    /** Promotion time. */
    @Column(name = "created_at", nullable = false)
    public Instant createdAt;
}
