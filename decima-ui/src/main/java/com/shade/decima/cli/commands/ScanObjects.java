package com.shade.decima.cli.commands;

import com.google.gson.stream.JsonWriter;
import com.shade.decima.cli.RTTIJsonWriter;
import com.shade.decima.model.app.Project;
import com.shade.decima.model.archive.ArchiveFile;
import com.shade.decima.model.packfile.Packfile;
import com.shade.decima.model.rtti.RTTIClass;
import com.shade.decima.model.rtti.RTTICoreFile;
import com.shade.decima.model.rtti.RTTICoreFileReader.LoggingErrorHandlingStrategy;
import com.shade.decima.model.rtti.RTTIType;
import com.shade.decima.model.rtti.RTTIUtils;
import com.shade.decima.model.rtti.objects.RTTIObject;
import com.shade.decima.model.rtti.objects.RTTIReference;
import com.shade.decima.model.rtti.registry.RTTITypeRegistry;
import com.shade.decima.model.rtti.types.RTTITypeArray;
import com.shade.decima.model.rtti.types.RTTITypeEnum;
import com.shade.util.NotNull;
import com.shade.util.Nullable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import static picocli.CommandLine.Help.Visibility.ALWAYS;

@Command(name = "scan-objects", description = "Scans .core objects by file, type, UUID, or text as JSON", sortOptions = false)
public class ScanObjects implements Callable<Void> {
    private static final int MAX_TEXT_MATCHES_PER_OBJECT = 20;
    private static final int MAX_TEXT_VALUE_LENGTH = 512;
    private static final int MAX_TEXT_SCAN_DEPTH = 32;

    @Option(names = {"-p", "--project"}, required = true, description = "The working project")
    private Project project;

    @Option(names = {"-o", "--output"}, description = "The output file (.json). Writes to stdout when omitted.")
    private Path output;

    @Option(names = {"-f", "--file"}, description = "Only scan this exact project file path. Can be specified multiple times.")
    private List<String> files = new ArrayList<>();

    @Option(names = {"--file-filter"}, description = "Only scan project file paths containing this text, case-insensitive. Can be specified multiple times.")
    private List<String> fileFilters = new ArrayList<>();

    @Option(names = {"-t", "--type"}, description = "Only include objects whose type is or derives from this RTTI class. Can be specified multiple times.")
    private Set<String> types = new HashSet<>();

    @Option(names = {"--type-filter"}, description = "Only include object type names containing this text, case-insensitive. Can be specified multiple times.")
    private List<String> typeFilters = new ArrayList<>();

    @Option(names = {"--uuid"}, description = "Only include objects with these UUIDs. Can be specified multiple times.")
    private Set<String> uuids = new HashSet<>();

    @Option(names = {"--text"}, description = "Only include objects with string, enum, or reference text containing this text. Can be specified multiple times.")
    private List<String> textFilters = new ArrayList<>();

    @Option(names = {"--values"}, description = "Include parsed object values", negatable = true, showDefaultValue = ALWAYS)
    private boolean values;

    @Option(names = {"--limit"}, description = "Maximum number of matching objects to write. Use 0 for no limit.", showDefaultValue = ALWAYS)
    private int limit;

    @Override
    public Void call() throws Exception {
        if (limit < 0) {
            throw new IllegalArgumentException("--limit must be >= 0");
        }

        final RTTITypeRegistry registry = project.getTypeRegistry();
        for (String type : types) {
            registry.find(type);
        }

        final Search search = new Search(Set.copyOf(types), normalize(typeFilters), normalize(uuids), normalize(textFilters));
        final List<String> candidates = getCandidateFiles();
        final List<ScanError> errors = new ArrayList<>();
        final ScanStats stats = new ScanStats(candidates.size());

        final Writer target = openOutput();
        try {
            final JsonWriter writer = new JsonWriter(target);
            writer.setLenient(false);
            writer.setIndent("\t");

            writer.beginObject();
            writer.name("project").value(project.getContainer().getName());
            writer.name("game").value(project.getContainer().getType().name());
            writeQuery(writer);

            writer.name("files");
            writer.beginArray();
            for (String path : candidates) {
                if (isLimitReached(stats)) {
                    stats.truncated = true;
                    break;
                }

                scanFile(path, search, stats, errors, writer);
            }
            writer.endArray();

            writeErrors(errors, writer);
            writeSummary(stats, errors, writer);
            writer.endObject();
            writer.flush();
        } finally {
            if (output != null) {
                target.close();
            }
        }

        return null;
    }

    @NotNull
    private Writer openOutput() throws IOException {
        if (output != null) {
            return Files.newBufferedWriter(output, StandardCharsets.UTF_8);
        }

        return new OutputStreamWriter(System.out, StandardCharsets.UTF_8);
    }

    @NotNull
    private List<String> getCandidateFiles() throws IOException {
        if (!files.isEmpty()) {
            return files.stream()
                .map(Packfile::getNormalizedPath)
                .filter(this::matchesFileFilters)
                .sorted()
                .toList();
        }

        try (Stream<String> stream = project.listAllFiles()) {
            return stream.map(Packfile::getNormalizedPath)
                .filter(ScanObjects::isCoreFile)
                .filter(this::matchesFileFilters)
                .distinct()
                .sorted()
                .toList();
        }
    }

    private boolean matchesFileFilters(@NotNull String path) {
        final String normalized = path.toLowerCase(Locale.ROOT);

        for (String filter : fileFilters) {
            if (!normalized.contains(filter.toLowerCase(Locale.ROOT))) {
                return false;
            }
        }

        return true;
    }

    private static boolean isCoreFile(@NotNull String path) {
        return path.endsWith(".core") || path.endsWith(".core.stream") || path.endsWith(".streaming.core");
    }

    private void scanFile(
        @NotNull String path,
        @NotNull Search search,
        @NotNull ScanStats stats,
        @NotNull List<ScanError> errors,
        @NotNull JsonWriter writer
    ) throws IOException {
        final ArchiveFile archiveFile = project.getPackfileManager().findFile(path);

        if (archiveFile == null) {
            errors.add(new ScanError(path, "File is not present in mounted packfiles"));
            return;
        }

        stats.scannedFiles++;

        final byte[] data;
        final List<EntryInfo> entries;
        final RTTICoreFile core;

        try {
            data = archiveFile.readAllBytes();
            entries = readEntries(data);
            core = project.getCoreFileReader().read(
                new ByteArrayInputStream(data),
                LoggingErrorHandlingStrategy.getInstance()
            );
        } catch (Exception e) {
            errors.add(new ScanError(path, e.getMessage() != null ? e.getMessage() : e.getClass().getName()));
            return;
        }

        final List<ObjectMatch> matches = new ArrayList<>();

        for (int i = 0; i < core.objects().size(); i++) {
            if (isLimitReached(stats)) {
                stats.truncated = true;
                break;
            }

            final RTTIObject object = core.objects().get(i);
            final ObjectMatch match = matchObject(object, getEntry(entries, i), search);

            if (match != null) {
                matches.add(match);
                stats.matchedObjects++;
            }
        }

        if (!matches.isEmpty()) {
            stats.matchedFiles++;
            writeFile(path, archiveFile, data.length, matches, writer);
        }
    }

    @Nullable
    private ObjectMatch matchObject(@NotNull RTTIObject object, @Nullable EntryInfo entry, @NotNull Search search) {
        final String type = object.type().getFullTypeName();
        final String normalizedType = type.toLowerCase(Locale.ROOT);

        if (!search.types().isEmpty() && search.types().stream().noneMatch(object.type()::isInstanceOf)) {
            return null;
        }

        for (String filter : search.typeFilters()) {
            if (!normalizedType.contains(filter)) {
                return null;
            }
        }

        final String uuid = getUUID(object);
        if (!search.uuids().isEmpty() && (uuid == null || !search.uuids().contains(uuid.toLowerCase(Locale.ROOT)))) {
            return null;
        }

        final List<TextMatch> textMatches = new ArrayList<>();
        if (!search.textFilters().isEmpty()) {
            final Set<String> matchedTextFilters = new HashSet<>();
            collectTextMatches(
                object,
                object.type(),
                "$",
                search.textFilters(),
                matchedTextFilters,
                textMatches,
                Collections.newSetFromMap(new IdentityHashMap<>()),
                0
            );

            if (!matchedTextFilters.containsAll(search.textFilters())) {
                return null;
            }
        }

        return new ObjectMatch(object, entry, uuid, List.copyOf(textMatches));
    }

    private static void collectTextMatches(
        @Nullable Object value,
        @NotNull RTTIType<?> type,
        @NotNull String path,
        @NotNull Set<String> filters,
        @NotNull Set<String> matchedFilters,
        @NotNull List<TextMatch> matches,
        @NotNull Set<Object> visited,
        int depth
    ) {
        if (value == null || depth > MAX_TEXT_SCAN_DEPTH || matchedFilters.containsAll(filters) && matches.size() >= MAX_TEXT_MATCHES_PER_OBJECT) {
            return;
        }

        if (value instanceof String text) {
            matchTextValue(path, text, filters, matchedFilters, matches);
        } else if (value instanceof RTTITypeEnum.Constant constant) {
            matchTextValue(path, constant.name(), filters, matchedFilters, matches);
        } else if (value instanceof RTTIReference.External ref) {
            matchTextValue(path + ".path", ref.path(), filters, matchedFilters, matches);
            matchTextValue(path + ".uuid", safeUUID(ref.uuid()), filters, matchedFilters, matches);
        } else if (value instanceof RTTIReference.Internal ref) {
            matchTextValue(path + ".uuid", safeUUID(ref.uuid()), filters, matchedFilters, matches);
        } else if (value instanceof RTTIObject object) {
            if (!visited.add(object.data())) {
                return;
            }

            for (RTTIClass.Field<?> field : object.type().getFields()) {
                final Object fieldValue;
                try {
                    fieldValue = field.get(object);
                } catch (RuntimeException ignored) {
                    continue;
                }

                collectTextMatches(fieldValue, field.getType(), path + "." + field.getName(), filters, matchedFilters, matches, visited, depth + 1);
            }
        } else if (type instanceof RTTITypeArray<?> array && isTextSearchable(array.getComponentType())) {
            final int length;
            try {
                length = array.length(value);
            } catch (RuntimeException e) {
                return;
            }

            for (int i = 0; i < length; i++) {
                final Object item;
                try {
                    item = array.get(value, i);
                } catch (RuntimeException e) {
                    continue;
                }

                collectTextMatches(item, array.getComponentType(), path + "[" + i + "]", filters, matchedFilters, matches, visited, depth + 1);
                if (matchedFilters.containsAll(filters) && matches.size() >= MAX_TEXT_MATCHES_PER_OBJECT) {
                    break;
                }
            }
        }
    }

    private static boolean isTextSearchable(@NotNull RTTIType<?> type) {
        final Class<?> cls = type.getInstanceType();
        return cls == String.class
            || cls == RTTIObject.class
            || RTTIReference.class.isAssignableFrom(cls)
            || RTTITypeEnum.Constant.class.isAssignableFrom(cls);
    }

    private static void matchTextValue(
        @NotNull String path,
        @NotNull String value,
        @NotNull Set<String> filters,
        @NotNull Set<String> matchedFilters,
        @NotNull List<TextMatch> matches
    ) {
        final String normalized = value.toLowerCase(Locale.ROOT);

        for (String filter : filters) {
            if (normalized.contains(filter)) {
                matchedFilters.add(filter);
                if (matches.size() < MAX_TEXT_MATCHES_PER_OBJECT) {
                    matches.add(new TextMatch(path, filter, sample(value)));
                }
            }
        }
    }

    @NotNull
    private static String sample(@NotNull String value) {
        if (value.length() <= MAX_TEXT_VALUE_LENGTH) {
            return value;
        }

        return value.substring(0, MAX_TEXT_VALUE_LENGTH) + "...";
    }

    @NotNull
    private static Set<String> normalize(@NotNull Iterable<String> values) {
        final Set<String> result = new HashSet<>();

        for (String value : values) {
            result.add(value.toLowerCase(Locale.ROOT));
        }

        return result;
    }

    @Nullable
    private static String getUUID(@NotNull RTTIObject object) {
        try {
            return RTTIUtils.uuidToString(object.uuid());
        } catch (RuntimeException e) {
            return null;
        }
    }

    @NotNull
    private static String safeUUID(@NotNull RTTIObject object) {
        try {
            return RTTIUtils.uuidToString(object);
        } catch (RuntimeException e) {
            return "";
        }
    }

    @NotNull
    private static List<EntryInfo> readEntries(byte[] data) throws IOException {
        final ByteBuffer buffer = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        final List<EntryInfo> entries = new ArrayList<>();

        while (buffer.hasRemaining()) {
            final int recordOffset = buffer.position();

            if (buffer.remaining() < 12) {
                throw new IOException("Unexpected end of stream while reading object header at offset " + recordOffset);
            }

            final long typeHash = buffer.getLong();
            final int dataSize = buffer.getInt();

            if (dataSize < 0 || buffer.remaining() < dataSize) {
                throw new IOException("Invalid object size " + dataSize + " at offset " + recordOffset);
            }

            entries.add(new EntryInfo(entries.size(), recordOffset, typeHash, dataSize));
            buffer.position(buffer.position() + dataSize);
        }

        return entries;
    }

    @Nullable
    private static EntryInfo getEntry(@NotNull List<EntryInfo> entries, int index) {
        return index < entries.size() ? entries.get(index) : null;
    }

    private boolean isLimitReached(@NotNull ScanStats stats) {
        return limit > 0 && stats.matchedObjects >= limit;
    }

    private void writeQuery(@NotNull JsonWriter writer) throws IOException {
        writer.name("query");
        writer.beginObject();
        writeStringArray("files", files, writer);
        writeStringArray("fileFilters", fileFilters, writer);
        writeStringArray("types", types.stream().sorted().toList(), writer);
        writeStringArray("typeFilters", typeFilters, writer);
        writeStringArray("uuids", uuids.stream().sorted().toList(), writer);
        writeStringArray("textFilters", textFilters, writer);
        writer.name("values").value(values);
        writer.name("limit").value(limit);
        writer.endObject();
    }

    private void writeFile(
        @NotNull String path,
        @NotNull ArchiveFile archiveFile,
        int size,
        @NotNull List<ObjectMatch> matches,
        @NotNull JsonWriter writer
    ) throws IOException {
        writer.beginObject();
        writer.name("path").value(path);
        writer.name("hash").value(formatHash(archiveFile.getIdentifier()));
        writer.name("archive").value(archiveFile.getArchive().getName());
        writer.name("size").value(size);

        writer.name("objects");
        writer.beginArray();
        for (ObjectMatch match : matches) {
            writeObject(project.getTypeRegistry(), match, writer);
        }
        writer.endArray();

        writer.endObject();
    }

    private void writeObject(
        @NotNull RTTITypeRegistry registry,
        @NotNull ObjectMatch match,
        @NotNull JsonWriter writer
    ) throws IOException {
        final RTTIObject object = match.object();

        writer.beginObject();
        writer.name("type").value(object.type().getFullTypeName());
        writeTypeHash(registry, object.type(), writer);

        if (match.entry() != null) {
            writer.name("index").value(match.entry().index());
            writer.name("recordOffset").value(match.entry().recordOffset());
            writer.name("dataOffset").value(match.entry().dataOffset());
            writer.name("dataSize").value(match.entry().dataSize());
            writer.name("recordTypeHash").value(formatHash(match.entry().typeHash()));
        }

        if (match.uuid() != null) {
            writer.name("uuid").value(match.uuid());
        }

        if (!match.textMatches().isEmpty()) {
            writer.name("textMatches");
            writer.beginArray();
            for (TextMatch textMatch : match.textMatches()) {
                writer.beginObject();
                writer.name("path").value(textMatch.path());
                writer.name("query").value(textMatch.query());
                writer.name("value").value(textMatch.value());
                writer.endObject();
            }
            writer.endArray();
        }

        if (values) {
            writer.name("value");
            RTTIJsonWriter.writeValue(object, object.type(), writer);
        }

        writer.endObject();
    }

    private static void writeTypeHash(@NotNull RTTITypeRegistry registry, @NotNull RTTIClass type, @NotNull JsonWriter writer) throws IOException {
        try {
            writer.name("typeHash").value(formatHash(registry.getHash(type)));
        } catch (IllegalArgumentException ignored) {
            // Some internal helper classes do not have serialized type hashes.
        }
    }

    private static void writeErrors(@NotNull List<ScanError> errors, @NotNull JsonWriter writer) throws IOException {
        writer.name("errors");
        writer.beginArray();
        for (ScanError error : errors) {
            writer.beginObject();
            writer.name("path").value(error.path());
            writer.name("message").value(error.message());
            writer.endObject();
        }
        writer.endArray();
    }

    private static void writeSummary(@NotNull ScanStats stats, @NotNull List<ScanError> errors, @NotNull JsonWriter writer) throws IOException {
        writer.name("summary");
        writer.beginObject();
        writer.name("candidateFiles").value(stats.candidateFiles());
        writer.name("scannedFiles").value(stats.scannedFiles);
        writer.name("matchedFiles").value(stats.matchedFiles);
        writer.name("matchedObjects").value(stats.matchedObjects);
        writer.name("errors").value(errors.size());
        writer.name("truncated").value(stats.truncated);
        writer.endObject();
    }

    private static void writeStringArray(@NotNull String name, @NotNull Iterable<String> values, @NotNull JsonWriter writer) throws IOException {
        writer.name(name);
        writer.beginArray();
        for (String value : values) {
            writer.value(value);
        }
        writer.endArray();
    }

    private static String formatHash(long hash) {
        return "0x%016x".formatted(hash);
    }

    private record Search(
        @NotNull Set<String> types,
        @NotNull Set<String> typeFilters,
        @NotNull Set<String> uuids,
        @NotNull Set<String> textFilters
    ) {
    }

    private record ObjectMatch(
        @NotNull RTTIObject object,
        @Nullable EntryInfo entry,
        @Nullable String uuid,
        @NotNull List<TextMatch> textMatches
    ) {
    }

    private record TextMatch(@NotNull String path, @NotNull String query, @NotNull String value) {
    }

    private record ScanError(@NotNull String path, @NotNull String message) {
    }

    private record EntryInfo(int index, int recordOffset, long typeHash, int dataSize) {
        int dataOffset() {
            return recordOffset + 12;
        }
    }

    private static final class ScanStats {
        private final int candidateFiles;
        private int scannedFiles;
        private int matchedFiles;
        private int matchedObjects;
        private boolean truncated;

        private ScanStats(int candidateFiles) {
            this.candidateFiles = candidateFiles;
        }

        private int candidateFiles() {
            return candidateFiles;
        }
    }
}
