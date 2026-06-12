package ai.pipestream.registration.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EmbeddedId;
import jakarta.persistence.Embeddable;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * One writable metadata overlay: a (scope, graph, type) triple holding the
 * field annotations for that layer. GLOBAL_DRAFT rows use an empty graph_id.
 * The {@code fields} column is a JSON envelope (SaveTypeMetadataRequest's
 * fields map) written via protobuf JsonFormat.
 */
@Entity
@Table(name = "type_metadata_overlays")
public class TypeMetadataOverlay extends PanacheEntityBase {

    /** Creates an empty entity instance for JPA. */
    public TypeMetadataOverlay() {
    }

    /** Composite key: scope + graph + type. */
    @Embeddable
    public static class Key implements Serializable {

        /** Creates an empty key for JPA. */
        public Key() {
        }

        /**
         * Creates a key.
         *
         * @param scope the overlay scope ("GLOBAL_DRAFT" or "PIPELINE")
         * @param graphId the graph for PIPELINE scope; empty for global
         * @param messageFullName the annotated type
         */
        public Key(String scope, String graphId, String messageFullName) {
            this.scope = scope;
            this.graphId = graphId;
            this.messageFullName = messageFullName;
        }

        /** Overlay scope ("GLOBAL_DRAFT" or "PIPELINE"). */
        @Column(name = "scope", nullable = false, length = 32)
        public String scope;

        /** Graph for PIPELINE scope; empty string for global. */
        @Column(name = "graph_id", nullable = false, length = 255)
        public String graphId;

        /** The annotated message type. */
        @Column(name = "message_full_name", nullable = false, length = 512)
        public String messageFullName;

        @Override
        public boolean equals(Object o) {
            return o instanceof Key k
                    && Objects.equals(scope, k.scope)
                    && Objects.equals(graphId, k.graphId)
                    && Objects.equals(messageFullName, k.messageFullName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(scope, graphId, messageFullName);
        }
    }

    /** The overlay identity. */
    @EmbeddedId
    public Key key;

    /** JSON envelope of field_path → FieldMetadataEntry. */
    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "fields", nullable = false, columnDefinition = "JSONB")
    public String fieldsJson;

    /** Last save time. */
    @Column(name = "updated_at", nullable = false)
    public Instant updatedAt;
}
