package ai.pipestream.registration.metadata;

import ai.pipestream.platform.registration.v1.FieldMetadata;
import ai.pipestream.platform.registration.v1.FieldMetadataEntry;
import ai.pipestream.platform.registration.v1.GetTypeMetadataResponse;
import ai.pipestream.platform.registration.v1.MetadataLayer;
import ai.pipestream.platform.registration.v1.MetadataScope;
import ai.pipestream.platform.registration.v1.SaveTypeMetadataRequest;
import ai.pipestream.registration.descriptors.TypeDescriptorIndex;
import ai.pipestream.registration.entity.MetadataPendingBake;
import ai.pipestream.registration.entity.TypeMetadataOverlay;
import ai.pipestream.registration.repository.ApicurioRegistryClient;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.transaction.Transactional;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;

/**
 * The type metadata catalog: layered field annotations over message types.
 *
 * <p>Layer precedence, lowest to highest: BAKED (proto doc-comments from the
 * descriptor snapshot) &lt; GLOBAL (the promoted Apicurio overlay) &lt;
 * GLOBAL_DRAFT (DB) &lt; PIPELINE (DB, graph-scoped, participates only when a
 * graph is named). Merging is whole-entry per field path — a higher layer's
 * entry replaces the lower layer's entry for that path outright.
 *
 * <p>Saves are full-replace per scope and validated: the type must exist in
 * the descriptor snapshot and every field path must resolve against it
 * (annotations are metadata additions only; they can never reference fields
 * that do not exist). Promotion (phase 1) writes the global draft to the
 * Apicurio system of record — immediately visible to every merged read — and
 * queues a pending-bake record for the phase-2 git automation.
 */
@ApplicationScoped
public class TypeMetadataService {

    private static final Logger LOG = Logger.getLogger(TypeMetadataService.class);

    static final String SCOPE_GLOBAL_DRAFT = "GLOBAL_DRAFT";
    static final String SCOPE_PIPELINE = "PIPELINE";

    /** Apicurio artifact id prefix for promoted overlays. */
    static final String ARTIFACT_PREFIX = "type-metadata.";

    @Inject
    TypeDescriptorIndex descriptorIndex;

    @Inject
    ApicurioRegistryClient apicurio;

    /** Result of a save. */
    public record SaveResult(int fieldCount, Instant updatedAt) { }

    /** Result of a promotion. */
    public record PromoteResult(String apicurioVersion, String pendingBakeId) { }

    /**
     * Builds one type's merged view with per-layer provenance.
     *
     * @param messageFullName the type
     * @param graphId when non-blank, this graph's PIPELINE overlay participates
     * @return the merged response
     * @throws NoSuchElementException when the type is not in the snapshot
     */
    public GetTypeMetadataResponse getMerged(String messageFullName, String graphId) {
        requireKnownType(messageFullName);

        Map<String, FieldMetadata> merged = new HashMap<>();

        descriptorIndex.bakedFieldDescriptions(messageFullName).forEach((field, comment) ->
                merged.put(field, FieldMetadata.newBuilder()
                        .setEntry(FieldMetadataEntry.newBuilder().setDescription(comment))
                        .setLayer(MetadataLayer.METADATA_LAYER_BAKED)
                        .build()));

        String globalVersion = "";
        ApicurioRegistryClient.ArtifactContent global = readGlobalOverlay(messageFullName);
        if (global != null) {
            globalVersion = global.version() == null ? "" : global.version();
            overlay(merged, parseFields(new String(global.bytes(), StandardCharsets.UTF_8)),
                    MetadataLayer.METADATA_LAYER_GLOBAL);
        }

        findOverlay(SCOPE_GLOBAL_DRAFT, "", messageFullName).ifPresent(draft ->
                overlay(merged, parseFields(draft.fieldsJson), MetadataLayer.METADATA_LAYER_GLOBAL_DRAFT));

        if (graphId != null && !graphId.isBlank()) {
            findOverlay(SCOPE_PIPELINE, graphId, messageFullName).ifPresent(pipeline ->
                    overlay(merged, parseFields(pipeline.fieldsJson), MetadataLayer.METADATA_LAYER_PIPELINE));
        }

        return GetTypeMetadataResponse.newBuilder()
                .setMessageFullName(messageFullName)
                .putAllFields(merged)
                .setGlobalVersion(globalVersion)
                .build();
    }

    /**
     * Fully replaces one scope's annotations for a type. An empty map deletes.
     *
     * @param scope PIPELINE (requires graphId) or GLOBAL_DRAFT (graphId must be blank)
     * @param graphId the graph for PIPELINE scope
     * @param messageFullName the annotated type
     * @param fields field path → entry
     * @return the persisted state
     * @throws IllegalArgumentException on bad scope/graph combination or unknown field path
     * @throws NoSuchElementException when the type is not in the snapshot
     */
    @Transactional
    public SaveResult save(MetadataScope scope, String graphId, String messageFullName,
                           Map<String, FieldMetadataEntry> fields) {
        String scopeName = switch (scope) {
            case METADATA_SCOPE_PIPELINE -> {
                if (graphId == null || graphId.isBlank()) {
                    throw new IllegalArgumentException("PIPELINE scope requires graph_id");
                }
                yield SCOPE_PIPELINE;
            }
            case METADATA_SCOPE_GLOBAL_DRAFT -> {
                if (graphId != null && !graphId.isBlank()) {
                    throw new IllegalArgumentException("GLOBAL_DRAFT scope must not carry graph_id");
                }
                yield SCOPE_GLOBAL_DRAFT;
            }
            default -> throw new IllegalArgumentException("scope must be PIPELINE or GLOBAL_DRAFT");
        };
        requireKnownType(messageFullName);
        for (String fieldPath : fields.keySet()) {
            if (!descriptorIndex.isValidFieldPath(messageFullName, fieldPath)) {
                throw new IllegalArgumentException("field_path '" + fieldPath
                        + "' does not resolve against " + messageFullName
                        + " — annotations cannot reference fields that do not exist");
            }
        }

        TypeMetadataOverlay.Key key = new TypeMetadataOverlay.Key(
                scopeName, graphId == null ? "" : graphId, messageFullName);
        if (fields.isEmpty()) {
            Optional.ofNullable((TypeMetadataOverlay) TypeMetadataOverlay.findById(key))
                    .ifPresent(TypeMetadataOverlay::delete);
            return new SaveResult(0, Instant.now());
        }

        TypeMetadataOverlay entity = TypeMetadataOverlay.findById(key);
        if (entity == null) {
            entity = new TypeMetadataOverlay();
            entity.key = key;
        }
        entity.fieldsJson = printFields(fields);
        entity.updatedAt = Instant.now();
        entity.persist();
        return new SaveResult(fields.size(), entity.updatedAt);
    }

    /**
     * Promotes a type's global draft to the Apicurio system of record and
     * queues the phase-2 bake record. The draft is consumed (deleted) — its
     * content now serves from the GLOBAL layer.
     *
     * @param messageFullName the type whose draft to promote
     * @return the Apicurio version + pending-bake id
     * @throws NoSuchElementException when the type has no global draft
     */
    @Transactional
    public PromoteResult promote(String messageFullName) {
        TypeMetadataOverlay draft = findOverlay(SCOPE_GLOBAL_DRAFT, "", messageFullName)
                .orElseThrow(() -> new NoSuchElementException(
                        "no global draft to promote for " + messageFullName));

        ApicurioRegistryClient.SchemaRegistrationResponse written = apicurio
                .createOrUpdateSchemaWithArtifactId(ARTIFACT_PREFIX + messageFullName, null, draft.fieldsJson);
        String version = written.getVersion() == null ? "" : written.getVersion();

        MetadataPendingBake bake = new MetadataPendingBake();
        bake.id = UUID.randomUUID().toString();
        bake.messageFullName = messageFullName;
        bake.apicurioVersion = version;
        bake.fieldsJson = draft.fieldsJson;
        bake.status = "PENDING";
        bake.createdAt = Instant.now();
        bake.persist();

        draft.delete();
        LOG.infof("Promoted metadata for %s to Apicurio version %s (bake %s queued)",
                messageFullName, version, bake.id);
        return new PromoteResult(version, bake.id);
    }

    private void requireKnownType(String messageFullName) {
        if (descriptorIndex.lookup(messageFullName).isEmpty()) {
            throw new NoSuchElementException(
                    "No message type '" + messageFullName + "' in the descriptor snapshot");
        }
    }

    private ApicurioRegistryClient.ArtifactContent readGlobalOverlay(String messageFullName) {
        // Promoted overlays live in the platform schema group (where
        // createOrUpdateSchemaWithArtifactId writes); returns null when absent.
        return apicurio.getArtifactContentFromGroup(
                "ai.pipestream.schemas", ARTIFACT_PREFIX + messageFullName, null);
    }

    private static Optional<TypeMetadataOverlay> findOverlay(String scope, String graphId, String type) {
        return Optional.ofNullable(
                TypeMetadataOverlay.findById(new TypeMetadataOverlay.Key(scope, graphId, type)));
    }

    private static void overlay(Map<String, FieldMetadata> merged,
                                Map<String, FieldMetadataEntry> entries, MetadataLayer layer) {
        entries.forEach((field, entry) -> merged.put(field,
                FieldMetadata.newBuilder().setEntry(entry).setLayer(layer).build()));
    }

    private static String printFields(Map<String, FieldMetadataEntry> fields) {
        try {
            return JsonFormat.printer().print(
                    SaveTypeMetadataRequest.newBuilder().putAllFields(fields).build());
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("Failed to serialize metadata fields", e);
        }
    }

    private static Map<String, FieldMetadataEntry> parseFields(String json) {
        try {
            SaveTypeMetadataRequest.Builder envelope = SaveTypeMetadataRequest.newBuilder();
            JsonFormat.parser().ignoringUnknownFields().merge(json, envelope);
            return envelope.getFieldsMap();
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("Stored metadata fields are unreadable", e);
        }
    }
}
