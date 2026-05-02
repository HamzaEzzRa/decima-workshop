package com.shade.decima.cli.commands;

import com.google.gson.stream.JsonWriter;
import com.shade.decima.model.app.Project;
import com.shade.decima.model.packfile.Packfile;
import com.shade.util.NotNull;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static picocli.CommandLine.Help.Visibility.ALWAYS;

@Command(name = "find-files", description = "Finds known or mounted project files as JSON", sortOptions = false)
public class FindFiles implements Callable<Void> {
    @Option(names = {"-p", "--project"}, required = true, description = "The working project")
    private Project project;

    @Option(names = {"-o", "--output"}, description = "The output file (.json). Writes to stdout when omitted.")
    private Path output;

    @Option(names = {"-f", "--filter"}, description = "Only include known paths containing this text, case-insensitive. Can be specified multiple times.")
    private List<String> filters = new ArrayList<>();

    @Option(names = {"--regex"}, description = "Only include known paths matching this Java regular expression.")
    private Pattern regex;

    @Option(names = {"-e", "--extension"}, description = "Only include known paths ending with this extension. Can be specified multiple times.")
    private List<String> extensions = new ArrayList<>();

    @Option(names = {"--hash"}, description = "Only include these file hashes. Accepts decimal, 0x-prefixed hex, or raw hex. Can be specified multiple times.")
    private List<String> hashes = new ArrayList<>();

    @Option(names = {"-a", "--archive"}, description = "Only include mounted entries from archives whose id, name, or path contains this text, case-insensitive. Can be specified multiple times.")
    private List<String> archives = new ArrayList<>();

    @Option(names = {"--include-unmounted"}, description = "Include known metadata paths that are not present in the mounted archives.")
    private boolean includeUnmounted;

    @Option(names = {"--include-unknown-hashes"}, description = "Include mounted file hashes that are not present in Decima Workshop path metadata.")
    private boolean includeUnknownHashes;

    @Option(names = {"--limit"}, description = "Maximum number of file records to write. Use 0 for no limit.", showDefaultValue = ALWAYS)
    private int limit;

    @Override
    public Void call() throws Exception {
        if (limit < 0) {
            throw new IllegalArgumentException("--limit must be >= 0");
        }

        normalizeExtensions();

        final Set<Long> hashFilter = parseHashFilters();
        final List<Packfile> selectedArchives = project.getPackfileManager().getArchives().stream()
            .filter(this::matchesArchive)
            .sorted(Comparator.comparing(Packfile::getName))
            .toList();
        final Map<Long, List<FileLocation>> mountedFiles = getMountedFiles(selectedArchives);
        final Map<Long, List<String>> knownPaths = getKnownPaths();

        final Writer target = openOutput();
        try {
            final JsonWriter writer = new JsonWriter(target);
            writer.setLenient(false);
            writer.setIndent("\t");

            writer.beginObject();
            writer.name("project").value(project.getContainer().getName());
            writer.name("game").value(project.getContainer().getType().name());
            writeQuery(hashFilter, writer);

            final Counter counter = new Counter();

            writer.name("files");
            writer.beginArray();

            for (KnownFile file : getKnownFiles(knownPaths)) {
                if (!matchesKnownFile(file, hashFilter)) {
                    continue;
                }

                final List<FileLocation> locations = mountedFiles.getOrDefault(file.hash(), List.of());
                if (locations.isEmpty() && !includeUnmounted) {
                    continue;
                }
                if (isLimitReached(counter)) {
                    counter.truncated = true;
                    break;
                }

                writeFile(file.path(), file.hash(), locations, writer);
                counter.written++;
            }

            if (includeUnknownHashes || !hashFilter.isEmpty()) {
                for (Map.Entry<Long, List<FileLocation>> entry : mountedFiles.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey(Long::compareUnsigned))
                    .toList()) {
                    if (knownPaths.containsKey(entry.getKey())) {
                        continue;
                    }
                    if (!matchesUnknownHash(entry.getKey(), hashFilter)) {
                        continue;
                    }
                    if (isLimitReached(counter)) {
                        counter.truncated = true;
                        break;
                    }

                    writeFile(null, entry.getKey(), entry.getValue(), writer);
                    counter.written++;
                }
            }

            writer.endArray();
            writer.name("truncated").value(counter.truncated);
            writer.name("count").value(counter.written);
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

    private void normalizeExtensions() {
        extensions = extensions.stream()
            .map(value -> value.toLowerCase(Locale.ROOT))
            .map(value -> value.startsWith(".") ? value : "." + value)
            .toList();
    }

    @NotNull
    private Set<Long> parseHashFilters() {
        final Set<Long> result = new HashSet<>();

        for (String value : hashes) {
            result.add(parseUnsignedLong(value));
        }

        return result;
    }

    private static long parseUnsignedLong(@NotNull String value) {
        String normalized = value.trim().replace("_", "");
        int radix = 10;

        if (normalized.startsWith("0x") || normalized.startsWith("0X")) {
            normalized = normalized.substring(2);
            radix = 16;
        } else if (normalized.matches(".*[a-fA-F].*")) {
            radix = 16;
        }

        return Long.parseUnsignedLong(normalized, radix);
    }

    @NotNull
    private Map<Long, List<FileLocation>> getMountedFiles(@NotNull Collection<Packfile> archives) {
        final Map<Long, List<FileLocation>> result = new HashMap<>();

        for (Packfile packfile : archives) {
            for (Packfile.FileEntry entry : packfile.getFileEntries()) {
                result.computeIfAbsent(entry.hash(), key -> new ArrayList<>()).add(new FileLocation(packfile, entry));
            }
        }

        for (List<FileLocation> locations : result.values()) {
            locations.sort(Comparator
                .comparing(FileLocation::packfile, Comparator.reverseOrder())
                .thenComparingLong(location -> location.entry().index()));
        }

        return result;
    }

    @NotNull
    private Map<Long, List<String>> getKnownPaths() throws IOException {
        final Map<Long, Set<String>> paths = new LinkedHashMap<>();

        try (Stream<String> stream = project.listAllFiles()) {
            stream.map(Packfile::getNormalizedPath)
                .forEach(path -> paths.computeIfAbsent(Packfile.getPathHash(path), key -> new TreeSet<>()).add(path));
        }

        final Map<Long, List<String>> result = new LinkedHashMap<>();
        for (Map.Entry<Long, Set<String>> entry : paths.entrySet()) {
            result.put(entry.getKey(), List.copyOf(entry.getValue()));
        }

        return result;
    }

    @NotNull
    private static List<KnownFile> getKnownFiles(@NotNull Map<Long, List<String>> knownPaths) {
        return knownPaths.entrySet().stream()
            .flatMap(entry -> entry.getValue().stream().map(path -> new KnownFile(path, entry.getKey())))
            .sorted(Comparator.comparing(KnownFile::path))
            .toList();
    }

    private boolean matchesKnownFile(@NotNull KnownFile file, @NotNull Set<Long> hashFilter) {
        if (!hashFilter.isEmpty() && !hashFilter.contains(file.hash())) {
            return false;
        }

        final String path = file.path().toLowerCase(Locale.ROOT);

        for (String filter : filters) {
            if (!path.contains(filter.toLowerCase(Locale.ROOT))) {
                return false;
            }
        }

        if (regex != null && !regex.matcher(file.path()).find()) {
            return false;
        }

        if (!extensions.isEmpty() && extensions.stream().noneMatch(path::endsWith)) {
            return false;
        }

        return true;
    }

    private boolean matchesUnknownHash(long hash, @NotNull Set<Long> hashFilter) {
        if (!hashFilter.isEmpty()) {
            return hashFilter.contains(hash);
        }

        return includeUnknownHashes && filters.isEmpty() && regex == null && extensions.isEmpty();
    }

    private boolean matchesArchive(@NotNull Packfile packfile) {
        if (archives.isEmpty()) {
            return true;
        }

        final String haystack = (packfile.getId() + '\n' + packfile.getName() + '\n' + packfile.getPath())
            .toLowerCase(Locale.ROOT);

        for (String archive : archives) {
            if (!haystack.contains(archive.toLowerCase(Locale.ROOT))) {
                return false;
            }
        }

        return true;
    }

    private boolean isLimitReached(@NotNull Counter counter) {
        return limit > 0 && counter.written >= limit;
    }

    private void writeQuery(@NotNull Set<Long> hashFilter, @NotNull JsonWriter writer) throws IOException {
        writer.name("query");
        writer.beginObject();
        writeStringArray("filters", filters, writer);
        writer.name("regex").value(regex != null ? regex.pattern() : null);
        writeStringArray("extensions", extensions, writer);
        writeHashArray("hashes", hashFilter, writer);
        writeStringArray("archives", archives, writer);
        writer.name("includeUnmounted").value(includeUnmounted);
        writer.name("includeUnknownHashes").value(includeUnknownHashes);
        writer.name("limit").value(limit);
        writer.endObject();
    }

    private static void writeFile(String path, long hash, @NotNull List<FileLocation> locations, @NotNull JsonWriter writer) throws IOException {
        writer.beginObject();
        writer.name("path").value(path);
        writer.name("hash").value(formatHash(hash));
        writer.name("mounted").value(!locations.isEmpty());

        if (path != null) {
            final String extension = getExtension(path);
            if (extension != null) {
                writer.name("extension").value(extension);
            }
        }

        writer.name("archives");
        writer.beginArray();
        for (FileLocation location : locations) {
            writeLocation(location, writer);
        }
        writer.endArray();

        writer.endObject();
    }

    private static void writeLocation(@NotNull FileLocation location, @NotNull JsonWriter writer) throws IOException {
        final Packfile packfile = location.packfile();
        final Packfile.FileEntry entry = location.entry();

        writer.beginObject();
        writer.name("id").value(packfile.getId());
        writer.name("name").value(packfile.getName());
        writer.name("path").value(packfile.getPath().toString());
        writer.name("entryIndex").value(entry.index());
        writer.name("offset").value(entry.span().offset());
        writer.name("size").value(entry.span().size());
        writer.endObject();
    }

    private static void writeStringArray(@NotNull String name, @NotNull Collection<String> values, @NotNull JsonWriter writer) throws IOException {
        writer.name(name);
        writer.beginArray();
        for (String value : values) {
            writer.value(value);
        }
        writer.endArray();
    }

    private static void writeHashArray(@NotNull String name, @NotNull Collection<Long> values, @NotNull JsonWriter writer) throws IOException {
        writer.name(name);
        writer.beginArray();
        for (Long value : values.stream().sorted(Long::compareUnsigned).toList()) {
            writer.value(formatHash(value));
        }
        writer.endArray();
    }

    private static String formatHash(long hash) {
        return "0x%016x".formatted(hash);
    }

    private static String getExtension(@NotNull String path) {
        final int index = path.lastIndexOf('.');
        return index >= 0 ? path.substring(index).toLowerCase(Locale.ROOT) : null;
    }

    private record FileLocation(@NotNull Packfile packfile, @NotNull Packfile.FileEntry entry) {
    }

    private record KnownFile(@NotNull String path, long hash) {
    }

    private static final class Counter {
        private int written;
        private boolean truncated;
    }
}
