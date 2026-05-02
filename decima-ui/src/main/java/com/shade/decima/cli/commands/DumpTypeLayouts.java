package com.shade.decima.cli.commands;

import com.google.gson.stream.JsonWriter;
import com.shade.decima.model.app.Project;
import com.shade.decima.model.rtti.RTTIClass;
import com.shade.decima.model.rtti.RTTIEnum;
import com.shade.decima.model.rtti.RTTIType;
import com.shade.decima.model.rtti.registry.RTTITypeRegistry;
import com.shade.decima.model.rtti.types.RTTITypeClass;
import com.shade.decima.model.rtti.types.RTTITypeEnum;
import com.shade.util.NotNull;
import com.shade.util.Nullable;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.Callable;

@Command(name = "rtti-types", description = "Dumps RTTI class and enum layouts as JSON", sortOptions = false)
public class DumpTypeLayouts implements Callable<Void> {
    @Option(names = {"-p", "--project"}, required = true, description = "The working project")
    private Project project;

    @Option(names = {"-o", "--output"}, description = "The output file (.json). Writes to stdout when omitted.")
    private Path output;

    @Option(names = {"-t", "--type"}, description = "Only dump these exact type names. Can be specified multiple times.")
    private Set<String> types = new HashSet<>();

    @Option(names = {"-f", "--filter"}, description = "Only dump type names containing this text, case-insensitive.")
    private String filter;

    @Override
    public Void call() throws Exception {
        final RTTITypeRegistry registry = project.getTypeRegistry();

        for (String type : types) {
            registry.find(type);
        }

        final List<RTTIType<?>> allTypes = registry.getTypes().stream()
            .filter(this::matches)
            .sorted(Comparator.comparing(RTTIType::getFullTypeName))
            .toList();

        final Writer target = openOutput();
        try {
            final JsonWriter writer = new JsonWriter(target);
            writer.setLenient(false);
            writer.setIndent("\t");

            writer.beginObject();
            writer.name("project").value(project.getContainer().getName());
            writer.name("game").value(project.getContainer().getType().name());

            writer.name("classes");
            writer.beginArray();
            for (RTTIType<?> type : allTypes) {
                if (type instanceof RTTITypeClass cls) {
                    writeClass(registry, cls, writer);
                }
            }
            writer.endArray();

            writer.name("enums");
            writer.beginArray();
            for (RTTIType<?> type : allTypes) {
                if (type instanceof RTTITypeEnum enumeration) {
                    writeEnum(enumeration, writer);
                }
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

    private boolean matches(@NotNull RTTIType<?> type) {
        if (!types.isEmpty() && !types.contains(type.getFullTypeName())) {
            return false;
        }

        return filter == null || type.getFullTypeName().toLowerCase(Locale.ROOT).contains(filter.toLowerCase(Locale.ROOT));
    }

    private static void writeClass(@NotNull RTTITypeRegistry registry, @NotNull RTTITypeClass cls, @NotNull JsonWriter writer) throws IOException {
        writer.beginObject();
        writer.name("name").value(cls.getFullTypeName());
        writeTypeHash(registry, cls, writer);
        writer.name("version").value(cls.getVersion());
        writer.name("flags").value(cls.getFlags());

        writer.name("superclasses");
        writer.beginArray();
        for (RTTIClass.Superclass superclass : cls.getSuperclasses()) {
            writer.beginObject();
            writer.name("name").value(superclass.getType().getFullTypeName());
            if (superclass instanceof RTTITypeClass.MySuperclass info) {
                writer.name("offset").value(info.offset());
            }
            writer.endObject();
        }
        writer.endArray();

        writer.name("fields");
        writer.beginArray();
        for (RTTITypeClass.FieldWithOffset info : cls.getOrderedFields()) {
            writeField(info, writer);
        }
        writer.endArray();

        writer.name("messages");
        writer.beginArray();
        for (RTTIClass.Message<?> message : cls.getMessages()) {
            writer.beginObject();
            writer.name("name").value(message.getName());
            writer.name("hasHandler").value(message.getHandler() != null);
            writer.endObject();
        }
        writer.endArray();

        writer.endObject();
    }

    private static void writeField(@NotNull RTTITypeClass.FieldWithOffset info, @NotNull JsonWriter writer) throws IOException {
        final RTTITypeClass.MyField field = info.field();
        final int flags = field.flags();

        writer.beginObject();
        writer.name("name").value(field.getName());
        writer.name("type").value(field.getType().getFullTypeName());
        writer.name("parent").value(field.getParent().getFullTypeName());
        writer.name("offset").value(info.offset());
        writer.name("declaredOffset").value(field.getOffset());
        writer.name("flags").value(flags);
        writer.name("saveState").value((flags & RTTITypeClass.MyField.FLAG_SAVE_STATE) != 0);
        writer.name("nonHashable").value((flags & RTTITypeClass.MyField.FLAG_NON_HASHABLE) != 0);
        writer.name("nonReadable").value((flags & RTTITypeClass.MyField.FLAG_NON_READABLE) != 0);

        if (field.getCategory() != null) {
            writer.name("category").value(field.getCategory());
        }

        writer.endObject();
    }

    private static void writeEnum(@NotNull RTTITypeEnum enumeration, @NotNull JsonWriter writer) throws IOException {
        writer.beginObject();
        writer.name("name").value(enumeration.getFullTypeName());
        writer.name("size").value(enumeration.getSize());
        writer.name("flags").value(enumeration.isEnumSet());

        writer.name("values");
        writer.beginArray();
        for (RTTIEnum.Constant constant : enumeration.values()) {
            writer.beginObject();
            writer.name("name").value(constant.name());
            writer.name("value").value(constant.value());
            writer.endObject();
        }
        writer.endArray();

        writer.endObject();
    }

    private static void writeTypeHash(@NotNull RTTITypeRegistry registry, @NotNull RTTIType<?> type, @NotNull JsonWriter writer) throws IOException {
        final Long hash = getTypeHash(registry, type);

        if (hash != null) {
            writer.name("hash").value("0x%016x".formatted(hash));
        }
    }

    @Nullable
    private static Long getTypeHash(@NotNull RTTITypeRegistry registry, @NotNull RTTIType<?> type) {
        try {
            return registry.getHash(type);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
