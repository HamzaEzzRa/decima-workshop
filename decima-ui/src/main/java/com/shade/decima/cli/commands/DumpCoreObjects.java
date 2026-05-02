package com.shade.decima.cli.commands;

import com.google.gson.stream.JsonWriter;
import com.shade.decima.cli.RTTIJsonWriter;
import com.shade.decima.model.app.Project;
import com.shade.decima.model.archive.ArchiveFile;
import com.shade.decima.model.rtti.RTTIClass;
import com.shade.decima.model.rtti.RTTICoreFile;
import com.shade.decima.model.rtti.RTTICoreFileReader.LoggingErrorHandlingStrategy;
import com.shade.decima.model.rtti.RTTIUtils;
import com.shade.decima.model.rtti.objects.RTTIObject;
import com.shade.decima.model.rtti.registry.RTTITypeRegistry;
import com.shade.decima.model.rtti.types.RTTITypeClass;
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
import java.util.List;
import java.util.concurrent.Callable;

import static picocli.CommandLine.Help.Visibility.ALWAYS;

@Command(name = "core-objects", description = "Dumps parsed .core objects and top-level record offsets as JSON", sortOptions = false)
public class DumpCoreObjects implements Callable<Void> {
    @Option(names = {"-p", "--project"}, required = true, description = "The working project")
    private Project project;

    @Option(names = {"-f", "--file"}, description = "The project file path to read from mounted packfiles")
    private String file;

    @Option(names = {"-i", "--input"}, description = "A local .core file to read")
    private Path input;

    @Option(names = {"-o", "--output"}, description = "The output file (.json). Writes to stdout when omitted.")
    private Path output;

    @Option(names = {"--values"}, description = "Include parsed object values", negatable = true, showDefaultValue = ALWAYS)
    private boolean values = true;

    @Override
    public Void call() throws Exception {
        if ((file == null) == (input == null)) {
            throw new IllegalArgumentException("Specify exactly one of '--file' or '--input'");
        }

        final Source source = readSource();
        final List<EntryInfo> entries = readEntries(source.data());
        final RTTICoreFile core = project.getCoreFileReader().read(
            new ByteArrayInputStream(source.data()),
            LoggingErrorHandlingStrategy.getInstance()
        );

        final Writer target = openOutput();
        try {
            final JsonWriter writer = new JsonWriter(target);
            writer.setLenient(false);
            writer.setIndent("\t");

            writer.beginObject();
            writer.name("project").value(project.getContainer().getName());
            writer.name("game").value(project.getContainer().getType().name());
            writer.name("source");
            writeSource(source, writer);

            writer.name("objects");
            writer.beginArray();
            for (int i = 0; i < core.objects().size(); i++) {
                writeObject(project.getTypeRegistry(), core.objects().get(i), getEntry(entries, i), writer);
            }
            writer.endArray();

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
    private Source readSource() throws IOException {
        if (input != null) {
            return new Source("local", input.toString(), null, Files.readAllBytes(input));
        }

        final ArchiveFile archiveFile = project.getPackfileManager().getFile(file);
        return new Source("packfile", file, archiveFile.getArchive().getName(), archiveFile.readAllBytes());
    }

    private void writeObject(
        @NotNull RTTITypeRegistry registry,
        @NotNull RTTIObject object,
        @Nullable EntryInfo entry,
        @NotNull JsonWriter writer
    ) throws IOException {
        writer.beginObject();
        writer.name("index").value(entry != null ? entry.index() : -1);
        writer.name("type").value(object.type().getFullTypeName());
        writeTypeHash(registry, object.type(), writer);

        if (entry != null) {
            writer.name("recordOffset").value(entry.recordOffset());
            writer.name("dataOffset").value(entry.dataOffset());
            writer.name("dataSize").value(entry.dataSize());
            writer.name("recordTypeHash").value("0x%016x".formatted(entry.typeHash()));
        }

        final String uuid = getUUID(object);
        if (uuid != null) {
            writer.name("uuid").value(uuid);
        }

        if (object.type() instanceof RTTITypeClass cls) {
            writer.name("layout");
            writeLayout(cls, writer);
        }

        if (values) {
            writer.name("value");
            RTTIJsonWriter.writeValue(object, object.type(), writer);
        }

        writer.endObject();
    }

    private static void writeLayout(@NotNull RTTITypeClass cls, @NotNull JsonWriter writer) throws IOException {
        writer.beginArray();

        for (RTTITypeClass.FieldWithOffset info : cls.getOrderedFields()) {
            final RTTIClass.Field<?> field = info.field();

            writer.beginObject();
            writer.name("name").value(field.getName());
            writer.name("type").value(field.getType().getFullTypeName());
            writer.name("offset").value(info.offset());
            writer.endObject();
        }

        writer.endArray();
    }

    private static void writeSource(@NotNull Source source, @NotNull JsonWriter writer) throws IOException {
        writer.beginObject();
        writer.name("kind").value(source.kind());
        writer.name("path").value(source.path());
        writer.name("size").value(source.data().length);
        if (source.archive() != null) {
            writer.name("archive").value(source.archive());
        }
        writer.endObject();
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

    @Nullable
    private static String getUUID(@NotNull RTTIObject object) {
        try {
            return RTTIUtils.uuidToString(object.uuid());
        } catch (RuntimeException e) {
            return null;
        }
    }

    private static void writeTypeHash(@NotNull RTTITypeRegistry registry, @NotNull RTTIClass type, @NotNull JsonWriter writer) throws IOException {
        try {
            writer.name("typeHash").value("0x%016x".formatted(registry.getHash(type)));
        } catch (IllegalArgumentException ignored) {
            // Some internal helper classes do not have serialized type hashes.
        }
    }

    private record Source(@NotNull String kind, @NotNull String path, @Nullable String archive, @NotNull byte[] data) {
    }

    private record EntryInfo(int index, int recordOffset, long typeHash, int dataSize) {
        int dataOffset() {
            return recordOffset + 12;
        }
    }
}
