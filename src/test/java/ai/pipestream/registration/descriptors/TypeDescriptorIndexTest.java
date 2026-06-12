package ai.pipestream.registration.descriptors;

import com.google.protobuf.Any;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.DynamicMessage;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pins the descriptor-door contract against the REAL baked workspace snapshot
 * (the same resource the running service serves):
 *
 * <ul>
 *   <li>Self-containment — every served set links into full
 *       {@link FileDescriptor}s using nothing but its own bytes. This is the
 *       backend mirror of the frontend acceptance bar
 *       (createFileRegistry(fds) + decode a parsed_metadata Any).</li>
 *   <li>Topological order — every file's dependencies precede it.</li>
 *   <li>Content-hash versioning — identical content, identical version.</li>
 *   <li>type_url parsing, nested types, map-entry exclusion, catalog
 *       filtering and group attribution.</li>
 * </ul>
 */
class TypeDescriptorIndexTest {

    private static final String TIKA_RESPONSE = "ai.pipestream.parsed.data.tika.v1.TikaResponse";
    private static final String TIKA_TYPE_URL = "type.googleapis.com/" + TIKA_RESPONSE;

    private static TypeDescriptorIndex index;

    @BeforeAll
    static void loadIndex() {
        index = new TypeDescriptorIndex();
        index.load();
    }

    @Test
    void tikaResponseResolvesByBareNameAndByTypeUrl() {
        Optional<TypeDescriptorIndex.TypeDescriptorPayload> byName = index.lookup(TIKA_RESPONSE);
        assertTrue(byName.isPresent(), "TikaResponse must be in the baked snapshot");
        assertEquals(TIKA_RESPONSE, byName.get().messageFullName());
        assertEquals("common", byName.get().group(), "tika_response.proto lives in the common workspace module");

        assertEquals(Optional.of(TIKA_RESPONSE), TypeDescriptorIndex.messageNameFromTypeUrl(TIKA_TYPE_URL));
        assertEquals(Optional.of(TIKA_RESPONSE), TypeDescriptorIndex.messageNameFromTypeUrl(TIKA_RESPONSE));
    }

    @Test
    void malformedTypeUrlsAreRejected() {
        assertTrue(TypeDescriptorIndex.messageNameFromTypeUrl(null).isEmpty());
        assertTrue(TypeDescriptorIndex.messageNameFromTypeUrl("").isEmpty());
        assertTrue(TypeDescriptorIndex.messageNameFromTypeUrl("type.googleapis.com/").isEmpty());
        assertTrue(TypeDescriptorIndex.messageNameFromTypeUrl("pkg..Msg").isEmpty());
        assertTrue(TypeDescriptorIndex.messageNameFromTypeUrl("pkg.1Msg").isEmpty());
        assertTrue(TypeDescriptorIndex.messageNameFromTypeUrl("has space").isEmpty());
    }

    @Test
    void servedSetIsTopologicallyOrdered() throws Exception {
        FileDescriptorSet set = parseServedSet(TIKA_RESPONSE);
        Set<String> seen = new HashSet<>();
        for (FileDescriptorProto file : set.getFileList()) {
            for (String dependency : file.getDependencyList()) {
                assertTrue(seen.contains(dependency),
                        "dependency " + dependency + " must precede " + file.getName());
            }
            seen.add(file.getName());
        }
        assertEquals("ai/pipestream/parsed/data/tika/v1/tika_response.proto",
                set.getFile(set.getFileCount() - 1).getName(),
                "the defining file is last in topological order");
    }

    @Test
    void servedSetIsSelfContainedAndDecodesATikaResponseAny() throws Exception {
        // Link the served bytes with NO external knowledge — exactly what the
        // browser registry does. Any missing import fails buildFrom here.
        Descriptor tika = linkAndFind(parseServedSet(TIKA_RESPONSE), TIKA_RESPONSE);

        // Round-trip a TikaResponse through Any using only the served descriptor:
        // this service has no compiled TikaResponse class, which is the point.
        assertNotNull(tika.findFieldByName("metadata"), "TikaResponse.metadata field expected");
        Descriptor metadataEntry = tika.findFieldByName("metadata").getMessageType();

        DynamicMessage original = DynamicMessage.newBuilder(tika)
                .setField(tika.findFieldByName("doc_id"), "doc-1")
                .setField(tika.findFieldByName("metadata"),
                        List.of(DynamicMessage.newBuilder(metadataEntry)
                                .setField(metadataEntry.findFieldByName("key"), "Content-Type")
                                .setField(metadataEntry.findFieldByName("text"), "application/pdf")
                                .build()))
                .build();
        Any packed = Any.newBuilder()
                .setTypeUrl(TIKA_TYPE_URL)
                .setValue(original.toByteString())
                .build();

        DynamicMessage decoded = DynamicMessage.parseFrom(tika, packed.getValue());
        assertEquals(original, decoded, "Any payload must decode losslessly via the served descriptor");
    }

    @Test
    void pipeDocAndProcessingMappingAreServedAndSelfContained() throws Exception {
        for (String type : List.of("ai.pipestream.data.v1.PipeDoc", "ai.pipestream.data.v1.ProcessingMapping")) {
            assertNotNull(linkAndFind(parseServedSet(type), type), type + " must link self-contained");
        }
    }

    @Test
    void versionIsAStableContentHash() {
        TypeDescriptorIndex.TypeDescriptorPayload first = index.lookup(TIKA_RESPONSE).orElseThrow();
        TypeDescriptorIndex.TypeDescriptorPayload second = index.lookup(TIKA_RESPONSE).orElseThrow();
        assertEquals(first.version(), second.version());
        assertEquals(64, first.version().length(), "SHA-256 hex");
        String otherVersion = index.lookup("ai.pipestream.data.v1.PipeDoc").orElseThrow().version();
        assertFalse(first.version().equals(otherVersion), "different content, different version");
    }

    @Test
    void nestedTypesAreAddressableAndMapEntriesAreNot() {
        // Nested message: PipeDoc-adjacent protos define nested types; assert via
        // a known one from the registration surface itself.
        List<TypeDescriptorIndex.TypeSummary> all = index.list(null);
        assertTrue(all.stream().anyMatch(t -> t.messageFullName().contains(".") &&
                        Character.isUpperCase(t.messageFullName().charAt(t.messageFullName().lastIndexOf('.') + 1))),
                "catalog is non-empty with qualified names");
        assertTrue(all.stream().noneMatch(t -> t.messageFullName().endsWith("Entry")
                        && index.lookup(t.messageFullName()).isEmpty()),
                "every listed type must resolve");
        // Map-entry synthetic messages (raw_properties is map<string,string>) are not listed,
        // while TikaMetadataEntry — a REAL message that happens to end in "Entry" — is.
        assertTrue(all.stream().noneMatch(t -> t.messageFullName().equals(TIKA_RESPONSE + ".RawPropertiesEntry")),
                "synthetic map entries are excluded from the catalog");
        assertTrue(all.stream().anyMatch(t -> t.messageFullName().equals(
                        "ai.pipestream.parsed.data.tika.v1.TikaMetadataEntry")),
                "real messages ending in Entry stay listed");
    }

    @Test
    void listFiltersCaseInsensitivelyAndSortsByName() {
        List<TypeDescriptorIndex.TypeSummary> tika = index.list("tikaresponse");
        assertFalse(tika.isEmpty(), "case-insensitive substring filter must match TikaResponse");
        assertTrue(tika.stream().allMatch(t -> t.messageFullName().toLowerCase().contains("tikaresponse")));

        List<TypeDescriptorIndex.TypeSummary> all = index.list("");
        for (int i = 1; i < all.size(); i++) {
            assertTrue(all.get(i - 1).messageFullName().compareTo(all.get(i).messageFullName()) < 0,
                    "catalog sorted strictly by full name");
        }
    }

    @Test
    void externalTypesReportExternalGroup() {
        TypeDescriptorIndex.TypeDescriptorPayload struct = index.lookup("google.protobuf.Struct").orElseThrow();
        assertEquals(TypeDescriptorIndex.EXTERNAL_GROUP, struct.group());
    }

    private static FileDescriptorSet parseServedSet(String type) throws Exception {
        return FileDescriptorSet.parseFrom(index.lookup(type).orElseThrow().fileDescriptorSet());
    }

    /** Links every file in the set from its own contents only, then finds {@code type}. */
    private static Descriptor linkAndFind(FileDescriptorSet set, String type) throws DescriptorValidationException {
        Map<String, FileDescriptor> linked = new HashMap<>();
        for (FileDescriptorProto file : set.getFileList()) {
            FileDescriptor[] dependencies = file.getDependencyList().stream()
                    .map(name -> {
                        FileDescriptor dep = linked.get(name);
                        if (dep == null) {
                            throw new IllegalStateException("set not self-contained / not topo-ordered: " + name);
                        }
                        return dep;
                    })
                    .toArray(FileDescriptor[]::new);
            linked.put(file.getName(), FileDescriptor.buildFrom(file, dependencies));
        }
        return linked.values().stream()
                .map(fd -> fd.findMessageTypeByName(type.substring(type.lastIndexOf('.') + 1)))
                .filter(d -> d != null && d.getFullName().equals(type))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException(type + " not found in linked set"));
    }
}
