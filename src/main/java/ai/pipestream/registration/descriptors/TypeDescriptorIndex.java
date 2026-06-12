package ai.pipestream.registration.descriptors;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import jakarta.annotation.PostConstruct;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

/**
 * In-memory index over the full-workspace protobuf descriptor snapshot baked
 * into this service at build time ({@code descriptors/pipestream-workspace.desc},
 * a serialized {@link FileDescriptorSet} produced by {@code buf build} over the
 * entire pipestream-protos workspace).
 *
 * <p>Backs the descriptor door ({@code GetTypeDescriptor} /
 * {@code ListTypeDescriptors}): given a message full name, produces a
 * SELF-CONTAINED descriptor set — the defining file plus all transitive
 * imports, topologically ordered (dependencies before dependents) — that a
 * browser-side registry can consume without any further fetch.
 *
 * <p>Everything here works at the {@link FileDescriptorProto} level; no
 * {@code FileDescriptor} linking is performed, so the index is independent of
 * which message types are compiled into this service.
 *
 * <p>Versioning is content-addressed: the version of a type's descriptor set
 * is the SHA-256 of its serialized bytes. Identical content always hashes to
 * the same version, so clients can cache for as long as the version matches.
 */
@ApplicationScoped
public class TypeDescriptorIndex {

    private static final Logger LOG = Logger.getLogger(TypeDescriptorIndex.class);

    static final String DESCRIPTOR_RESOURCE = "/descriptors/pipestream-workspace.desc";
    static final String MANIFEST_RESOURCE = "/descriptors/proto-modules.properties";

    /** Workspace group reported for files that belong to no workspace module (e.g. well-known types). */
    static final String EXTERNAL_GROUP = "external";

    private static final Pattern MESSAGE_FULL_NAME = Pattern.compile(
            "[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*)*");

    /** One type's served descriptor set: the bytes, their content-hash version, and catalog metadata. */
    public record TypeDescriptorPayload(byte[] fileDescriptorSet, String version,
                                        String messageFullName, String group) { }

    /** Catalog row for {@code ListTypeDescriptors}. */
    public record TypeSummary(String messageFullName, String group, String version) { }

    private Map<String, FileDescriptorProto> filesByName;
    private Map<String, String> definingFileByType;
    private Map<String, String> moduleByFile;
    private final ConcurrentHashMap<String, TypeDescriptorPayload> payloadCache = new ConcurrentHashMap<>();

    /**
     * Loads and indexes the baked descriptor snapshot. The resource is produced
     * by this service's own build; a missing or unparsable snapshot is a broken
     * build and fails startup.
     */
    @PostConstruct
    public void load() {
        FileDescriptorSet set;
        try (InputStream in = TypeDescriptorIndex.class.getResourceAsStream(DESCRIPTOR_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException(
                        "Baked descriptor snapshot " + DESCRIPTOR_RESOURCE + " is missing from the classpath; "
                                + "the buildDescriptors task did not run or processResources did not copy it");
            }
            set = FileDescriptorSet.parseFrom(in);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read baked descriptor snapshot " + DESCRIPTOR_RESOURCE, e);
        }

        Map<String, FileDescriptorProto> files = new HashMap<>();
        Map<String, String> types = new HashMap<>();
        for (FileDescriptorProto file : set.getFileList()) {
            files.put(file.getName(), file);
            String packagePrefix = file.getPackage().isEmpty() ? "" : file.getPackage() + ".";
            for (DescriptorProto message : file.getMessageTypeList()) {
                indexMessage(types, file.getName(), packagePrefix, message);
            }
        }

        Map<String, String> modules = new HashMap<>();
        try (InputStream in = TypeDescriptorIndex.class.getResourceAsStream(MANIFEST_RESOURCE)) {
            if (in != null) {
                Properties props = new Properties();
                props.load(in);
                for (String fileName : props.stringPropertyNames()) {
                    modules.put(fileName, props.getProperty(fileName));
                }
            } else {
                LOG.warnf("Module manifest %s missing; all types will report group '%s'",
                        MANIFEST_RESOURCE, EXTERNAL_GROUP);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read module manifest " + MANIFEST_RESOURCE, e);
        }

        this.filesByName = Map.copyOf(files);
        this.definingFileByType = Map.copyOf(types);
        this.moduleByFile = Map.copyOf(modules);
        LOG.infof("Descriptor snapshot indexed: %d files, %d message types, %d module-manifest entries",
                files.size(), types.size(), modules.size());
    }

    private static void indexMessage(Map<String, String> types, String fileName,
                                     String namePrefix, DescriptorProto message) {
        // Synthetic map-entry messages are an encoding detail, not addressable types.
        if (message.getOptions().getMapEntry()) {
            return;
        }
        String fullName = namePrefix + message.getName();
        types.put(fullName, fileName);
        for (DescriptorProto nested : message.getNestedTypeList()) {
            indexMessage(types, fileName, fullName + ".", nested);
        }
    }

    /**
     * Extracts the message full name from an {@code Any} type_url
     * ({@code type.googleapis.com/pkg.Msg} — everything after the last slash)
     * or returns a bare name unchanged. Returns empty when the result is not
     * a syntactically valid fully-qualified message name.
     *
     * @param typeUrl the type_url or bare message name
     * @return the message full name, or empty if malformed
     */
    public static Optional<String> messageNameFromTypeUrl(String typeUrl) {
        if (typeUrl == null) {
            return Optional.empty();
        }
        String name = typeUrl.substring(typeUrl.lastIndexOf('/') + 1).trim();
        if (name.isEmpty() || !MESSAGE_FULL_NAME.matcher(name).matches()) {
            return Optional.empty();
        }
        return Optional.of(name);
    }

    /**
     * Resolves one message type to its self-contained descriptor set.
     *
     * @param messageFullName fully-qualified message name ({@code pkg.Msg})
     * @return the payload, or empty when the type is not in the snapshot
     */
    public Optional<TypeDescriptorPayload> lookup(String messageFullName) {
        String definingFile = definingFileByType.get(messageFullName);
        if (definingFile == null) {
            return Optional.empty();
        }
        return Optional.of(payloadCache.computeIfAbsent(messageFullName, name -> buildPayload(name, definingFile)));
    }

    /**
     * Lists every message type in the snapshot, sorted by full name.
     *
     * @param filter case-insensitive substring filter on the full name; null or blank = no filter
     * @return catalog rows including each type's current content-hash version
     */
    public List<TypeSummary> list(String filter) {
        String needle = filter == null ? "" : filter.trim().toLowerCase(Locale.ROOT);
        List<TypeSummary> result = new ArrayList<>();
        for (String fullName : definingFileByType.keySet().stream().sorted().toList()) {
            if (!needle.isEmpty() && !fullName.toLowerCase(Locale.ROOT).contains(needle)) {
                continue;
            }
            TypeDescriptorPayload payload = lookup(fullName).orElseThrow();
            result.add(new TypeSummary(fullName, payload.group(), payload.version()));
        }
        return result;
    }

    /**
     * Whether a dot-separated field path resolves against a type's schema.
     * Each segment must name a field of the current message; intermediate
     * segments must be message-typed (repeated message fields may be
     * traversed — the path describes schema, not data); synthetic map-entry
     * messages are not traversable (annotate the map field itself).
     *
     * @param messageFullName the root type
     * @param fieldPath dot-separated field names ("content.text", "doc_id")
     * @return true when every segment resolves
     */
    public boolean isValidFieldPath(String messageFullName, String fieldPath) {
        if (fieldPath == null || fieldPath.isBlank()) {
            return false;
        }
        DescriptorProto current = findMessage(messageFullName);
        if (current == null) {
            return false;
        }
        String[] segments = fieldPath.split("\\.");
        for (int i = 0; i < segments.length; i++) {
            FieldDescriptorProto field = findField(current, segments[i]);
            if (field == null) {
                return false;
            }
            if (i == segments.length - 1) {
                return true;
            }
            if (field.getType() != FieldDescriptorProto.Type.TYPE_MESSAGE) {
                return false;
            }
            DescriptorProto next = findMessage(stripLeadingDot(field.getTypeName()));
            if (next == null || next.getOptions().getMapEntry()) {
                return false;
            }
            current = next;
        }
        return false;
    }

    /**
     * The BAKED metadata layer: each direct field's leading proto doc-comment
     * (trailing as fallback), keyed by field name. Nested paths are served by
     * fetching the nested type's own metadata — annotations are type-scoped
     * and compose.
     *
     * @param messageFullName the type
     * @return field name → trimmed comment; empty map for unknown types or
     *         comment-less fields
     */
    public Map<String, String> bakedFieldDescriptions(String messageFullName) {
        String definingFile = definingFileByType.get(messageFullName);
        if (definingFile == null) {
            return Map.of();
        }
        FileDescriptorProto file = filesByName.get(definingFile);
        List<Integer> messagePath = pathToMessage(file, messageFullName);
        DescriptorProto message = findMessage(messageFullName);
        if (messagePath == null || message == null) {
            return Map.of();
        }

        Map<List<Integer>, String> comments = new HashMap<>();
        for (var location : file.getSourceCodeInfo().getLocationList()) {
            String comment = !location.getLeadingComments().isBlank()
                    ? location.getLeadingComments()
                    : location.getTrailingComments();
            if (!comment.isBlank()) {
                comments.put(location.getPathList(), comment.strip());
            }
        }

        Map<String, String> result = new HashMap<>();
        for (int f = 0; f < message.getFieldCount(); f++) {
            List<Integer> fieldPath = new ArrayList<>(messagePath);
            fieldPath.add(DescriptorProto.FIELD_FIELD_NUMBER);
            fieldPath.add(f);
            String comment = comments.get(fieldPath);
            if (comment != null) {
                result.put(message.getField(f).getName(), comment);
            }
        }
        return result;
    }

    /** Resolves a message's DescriptorProto by full name; null when unknown. */
    private DescriptorProto findMessage(String messageFullName) {
        String definingFile = definingFileByType.get(messageFullName);
        if (definingFile == null) {
            return null;
        }
        FileDescriptorProto file = filesByName.get(definingFile);
        String packagePrefix = file.getPackage().isEmpty() ? "" : file.getPackage() + ".";
        if (!messageFullName.startsWith(packagePrefix)) {
            return null;
        }
        String[] names = messageFullName.substring(packagePrefix.length()).split("\\.");
        DescriptorProto current = null;
        List<DescriptorProto> level = file.getMessageTypeList();
        for (String name : names) {
            current = level.stream().filter(m -> m.getName().equals(name)).findFirst().orElse(null);
            if (current == null) {
                return null;
            }
            level = current.getNestedTypeList();
        }
        return current;
    }

    /** SourceCodeInfo path to a message declaration ([4, m] / [4, m, 3, n] ...). */
    private List<Integer> pathToMessage(FileDescriptorProto file, String messageFullName) {
        String packagePrefix = file.getPackage().isEmpty() ? "" : file.getPackage() + ".";
        if (!messageFullName.startsWith(packagePrefix)) {
            return null;
        }
        String[] names = messageFullName.substring(packagePrefix.length()).split("\\.");
        List<Integer> path = new ArrayList<>();
        List<DescriptorProto> level = file.getMessageTypeList();
        for (int n = 0; n < names.length; n++) {
            int index = -1;
            for (int i = 0; i < level.size(); i++) {
                if (level.get(i).getName().equals(names[n])) {
                    index = i;
                    break;
                }
            }
            if (index < 0) {
                return null;
            }
            path.add(n == 0
                    ? FileDescriptorProto.MESSAGE_TYPE_FIELD_NUMBER
                    : DescriptorProto.NESTED_TYPE_FIELD_NUMBER);
            path.add(index);
            level = level.get(index).getNestedTypeList();
        }
        return path;
    }

    private static FieldDescriptorProto findField(DescriptorProto message, String name) {
        return message.getFieldList().stream()
                .filter(f -> f.getName().equals(name))
                .findFirst()
                .orElse(null);
    }

    private static String stripLeadingDot(String typeName) {
        return typeName.startsWith(".") ? typeName.substring(1) : typeName;
    }

    private TypeDescriptorPayload buildPayload(String messageFullName, String definingFile) {
        LinkedHashSet<String> ordered = new LinkedHashSet<>();
        collectTransitive(definingFile, ordered);

        FileDescriptorSet.Builder pruned = FileDescriptorSet.newBuilder();
        for (String fileName : ordered) {
            pruned.addFile(filesByName.get(fileName));
        }
        byte[] bytes = pruned.build().toByteArray();
        String group = moduleByFile.getOrDefault(definingFile, EXTERNAL_GROUP);
        return new TypeDescriptorPayload(bytes, sha256Hex(bytes), messageFullName, group);
    }

    /**
     * Post-order DFS over the import graph: each file is appended after all of
     * its dependencies, yielding a topological order (dependencies first). The
     * snapshot is buf-built, so every import is present and the graph is acyclic.
     */
    private void collectTransitive(String fileName, LinkedHashSet<String> ordered) {
        if (ordered.contains(fileName)) {
            return;
        }
        FileDescriptorProto file = filesByName.get(fileName);
        if (file == null) {
            throw new IllegalStateException(
                    "Descriptor snapshot is not self-contained: import '" + fileName + "' is missing. "
                            + "The buildDescriptors task must include imports.");
        }
        for (String dependency : file.getDependencyList()) {
            collectTransitive(dependency, ordered);
        }
        ordered.add(fileName);
    }

    private static String sha256Hex(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("JVM without SHA-256", e);
        }
    }
}
