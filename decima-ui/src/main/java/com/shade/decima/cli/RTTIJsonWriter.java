package com.shade.decima.cli;

import com.google.gson.stream.JsonWriter;
import com.shade.decima.model.rtti.RTTIClass;
import com.shade.decima.model.rtti.RTTIType;
import com.shade.decima.model.rtti.RTTIUtils;
import com.shade.decima.model.rtti.objects.RTTIObject;
import com.shade.decima.model.rtti.objects.RTTIReference;
import com.shade.decima.model.rtti.types.RTTITypeArray;
import com.shade.decima.model.rtti.types.RTTITypeEnum;
import com.shade.util.NotNull;
import com.shade.util.Nullable;

import java.io.IOException;

public final class RTTIJsonWriter {
    private RTTIJsonWriter() {
        // Utility class
    }

    public static void writeValue(@Nullable Object object, @NotNull RTTIType<?> type, @NotNull JsonWriter writer) throws IOException {
        if (object == null) {
            writer.nullValue();
        } else if (object instanceof RTTIObject obj) {
            writeObject(obj, writer);
        } else if (object instanceof RTTIReference.None) {
            writer.beginObject();
            writer.name("kind").value("none");
            writer.endObject();
        } else if (object instanceof RTTIReference.Internal ref) {
            writer.beginObject();
            writer.name("kind").value(ref.kind().name().toLowerCase());
            writer.name("target").value("internal");
            writer.name("uuid").value(RTTIUtils.uuidToString(ref.uuid()));
            writer.endObject();
        } else if (object instanceof RTTIReference.External ref) {
            writer.beginObject();
            writer.name("kind").value(ref.kind().name().toLowerCase());
            writer.name("target").value("external");
            writer.name("path").value(ref.path());
            writer.name("uuid").value(RTTIUtils.uuidToString(ref.uuid()));
            writer.endObject();
        } else if (type instanceof RTTITypeArray<?> array) {
            writer.beginArray();

            for (int i = 0, length = array.length(object); i < length; i++) {
                writeValue(array.get(object, i), array.getComponentType(), writer);
            }

            writer.endArray();
        } else if (object instanceof String value) {
            writer.value(value);
        } else if (object instanceof Number value) {
            writer.value(value);
        } else if (object instanceof Boolean value) {
            writer.value(value);
        } else if (object instanceof RTTITypeEnum.Constant constant) {
            writer.value(constant.name());
        } else {
            writer.value("<unsupported type '" + type.getFullTypeName() + "'>");
        }
    }

    private static void writeObject(@NotNull RTTIObject object, @NotNull JsonWriter writer) throws IOException {
        writer.beginObject();
        writer.name("$type").value(object.type().getFullTypeName());

        for (RTTIClass.Field<?> field : object.type().getFields()) {
            final Object value = field.get(object);

            if (value != null) {
                writer.name(field.getName());
                writeValue(value, field.getType(), writer);
            }
        }

        writer.endObject();
    }
}
