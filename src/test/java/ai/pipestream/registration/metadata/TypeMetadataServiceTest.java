package ai.pipestream.registration.metadata;

import ai.pipestream.platform.registration.v1.FieldMetadata;
import ai.pipestream.platform.registration.v1.FieldMetadataEntry;
import ai.pipestream.platform.registration.v1.FieldSensitivity;
import ai.pipestream.platform.registration.v1.GetTypeMetadataResponse;
import ai.pipestream.platform.registration.v1.MetadataLayer;
import ai.pipestream.platform.registration.v1.MetadataScope;
import ai.pipestream.registration.entity.MetadataPendingBake;
import ai.pipestream.registration.repository.ApicurioRegistryClient;
import io.quarkus.test.InjectMock;
import io.quarkus.test.junit.QuarkusTest;
import jakarta.inject.Inject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Pins the metadata catalog contract over the REAL baked descriptor snapshot:
 * layer precedence with provenance (BAKED &lt; GLOBAL &lt; GLOBAL_DRAFT &lt;
 * PIPELINE), proto doc-comments as the baked layer, full-replace/empty-deletes
 * save semantics, field-path validation against the descriptor, and promotion
 * phase 1 (Apicurio SOR write + pending-bake + draft consumption).
 */
@QuarkusTest
class TypeMetadataServiceTest {

    private static final String TIKA = "ai.pipestream.parsed.data.tika.v1.TikaResponse";

    @Inject
    TypeMetadataService service;

    @InjectMock
    ApicurioRegistryClient apicurio;

    @BeforeEach
    void noGlobalOverlayByDefault() {
        when(apicurio.getArtifactContentFromGroup(anyString(), anyString(), isNull()))
                .thenReturn(null);
    }

    private static FieldMetadataEntry entry(String displayName) {
        return FieldMetadataEntry.newBuilder()
                .setDisplayName(displayName)
                .setSensitivity(FieldSensitivity.FIELD_SENSITIVITY_INTERNAL)
                .addTags("test")
                .build();
    }

    @Test
    void bakedLayerServesProtoDocComments() {
        GetTypeMetadataResponse view = service.getMerged(TIKA, null);
        FieldMetadata docId = view.getFieldsMap().get("doc_id");
        assertNotNull(docId, "TikaResponse.doc_id carries a proto comment");
        assertEquals(MetadataLayer.METADATA_LAYER_BAKED, docId.getLayer());
        assertTrue(docId.getEntry().getDescription().contains("Document identifier"));
        assertEquals("", view.getGlobalVersion(), "no global overlay exists");
    }

    @Test
    void draftOverridesBakedWithProvenanceAndPipelineOverridesDraft() {
        service.save(MetadataScope.METADATA_SCOPE_GLOBAL_DRAFT, "", TIKA,
                Map.of("doc_id", entry("Draft Name")));
        service.save(MetadataScope.METADATA_SCOPE_PIPELINE, "graph-1", TIKA,
                Map.of("doc_id", entry("Pipeline Name")));

        GetTypeMetadataResponse withoutGraph = service.getMerged(TIKA, null);
        assertEquals(MetadataLayer.METADATA_LAYER_GLOBAL_DRAFT,
                withoutGraph.getFieldsMap().get("doc_id").getLayer());
        assertEquals("Draft Name", withoutGraph.getFieldsMap().get("doc_id").getEntry().getDisplayName());

        GetTypeMetadataResponse withGraph = service.getMerged(TIKA, "graph-1");
        assertEquals(MetadataLayer.METADATA_LAYER_PIPELINE,
                withGraph.getFieldsMap().get("doc_id").getLayer());
        assertEquals("Pipeline Name", withGraph.getFieldsMap().get("doc_id").getEntry().getDisplayName());

        GetTypeMetadataResponse otherGraph = service.getMerged(TIKA, "graph-other");
        assertEquals(MetadataLayer.METADATA_LAYER_GLOBAL_DRAFT,
                otherGraph.getFieldsMap().get("doc_id").getLayer(),
                "pipeline overlays are segregated per graph");

        // cleanup scope rows for other tests
        service.save(MetadataScope.METADATA_SCOPE_GLOBAL_DRAFT, "", TIKA, Map.of());
        service.save(MetadataScope.METADATA_SCOPE_PIPELINE, "graph-1", TIKA, Map.of());
    }

    @Test
    void globalLayerReadsApicurioOverlayWithVersion() {
        String overlayJson = "{\"fields\":{\"doc_id\":{\"displayName\":\"Promoted Name\"}}}";
        when(apicurio.getArtifactContentFromGroup(eq("ai.pipestream.schemas"),
                eq("type-metadata." + TIKA), isNull()))
                .thenReturn(new ApicurioRegistryClient.ArtifactContent(
                        overlayJson.getBytes(StandardCharsets.UTF_8), "application/json", "3"));

        GetTypeMetadataResponse view = service.getMerged(TIKA, null);
        assertEquals("3", view.getGlobalVersion());
        assertEquals(MetadataLayer.METADATA_LAYER_GLOBAL, view.getFieldsMap().get("doc_id").getLayer());
        assertEquals("Promoted Name", view.getFieldsMap().get("doc_id").getEntry().getDisplayName());
    }

    @Test
    void nestedFieldPathsValidateAcrossMessages() {
        TypeMetadataService.SaveResult saved = service.save(
                MetadataScope.METADATA_SCOPE_GLOBAL_DRAFT, "", TIKA,
                Map.of("content.body", entry("Body Text")));
        assertEquals(1, saved.fieldCount());
        assertEquals("Body Text",
                service.getMerged(TIKA, null).getFieldsMap().get("content.body").getEntry().getDisplayName());
        service.save(MetadataScope.METADATA_SCOPE_GLOBAL_DRAFT, "", TIKA, Map.of());
    }

    @Test
    void unknownFieldPathIsRejected() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () ->
                service.save(MetadataScope.METADATA_SCOPE_GLOBAL_DRAFT, "", TIKA,
                        Map.of("no_such_field", entry("x"))));
        assertTrue(e.getMessage().contains("no_such_field"));
    }

    @Test
    void unknownTypeIsRejected() {
        assertThrows(NoSuchElementException.class, () ->
                service.getMerged("ai.pipestream.no.such.Type", null));
        assertThrows(NoSuchElementException.class, () ->
                service.save(MetadataScope.METADATA_SCOPE_GLOBAL_DRAFT, "",
                        "ai.pipestream.no.such.Type", Map.of("x", entry("x"))));
    }

    @Test
    void scopeGraphCombinationsAreEnforced() {
        assertThrows(IllegalArgumentException.class, () ->
                service.save(MetadataScope.METADATA_SCOPE_PIPELINE, "", TIKA,
                        Map.of("doc_id", entry("x"))));
        assertThrows(IllegalArgumentException.class, () ->
                service.save(MetadataScope.METADATA_SCOPE_GLOBAL_DRAFT, "graph-1", TIKA,
                        Map.of("doc_id", entry("x"))));
        assertThrows(IllegalArgumentException.class, () ->
                service.save(MetadataScope.METADATA_SCOPE_UNSPECIFIED, "", TIKA,
                        Map.of("doc_id", entry("x"))));
    }

    @Test
    void promoteWritesApicurioQueuesBakeAndConsumesDraft() {
        when(apicurio.createOrUpdateSchemaWithArtifactId(eq("type-metadata." + TIKA), isNull(), anyString()))
                .thenReturn(new ApicurioRegistryClient.SchemaRegistrationResponse(
                        "type-metadata." + TIKA, 42L, "7"));

        service.save(MetadataScope.METADATA_SCOPE_GLOBAL_DRAFT, "", TIKA,
                Map.of("doc_id", entry("Promote Me")));
        TypeMetadataService.PromoteResult promoted = service.promote(TIKA);

        assertEquals("7", promoted.apicurioVersion());
        verify(apicurio).createOrUpdateSchemaWithArtifactId(
                eq("type-metadata." + TIKA), isNull(), any(String.class));

        // Draft consumed: the merged view no longer shows a GLOBAL_DRAFT layer.
        GetTypeMetadataResponse view = service.getMerged(TIKA, null);
        assertFalse(view.getFieldsMap().containsKey("doc_id")
                        && view.getFieldsMap().get("doc_id").getLayer()
                                == MetadataLayer.METADATA_LAYER_GLOBAL_DRAFT,
                "the promoted draft must be consumed");

        // Bake record queued with the promoted content.
        List<MetadataPendingBake> bakes = MetadataPendingBake
                .list("messageFullName = ?1 and id = ?2", TIKA, promoted.pendingBakeId());
        assertEquals(1, bakes.size());
        assertEquals("PENDING", bakes.get(0).status);
        assertEquals("7", bakes.get(0).apicurioVersion);
        assertTrue(bakes.get(0).fieldsJson.contains("Promote Me"));
    }

    @Test
    void promoteWithoutDraftIsNotFound() {
        assertThrows(NoSuchElementException.class, () ->
                service.promote("ai.pipestream.data.v1.PipeDoc"));
    }
}
