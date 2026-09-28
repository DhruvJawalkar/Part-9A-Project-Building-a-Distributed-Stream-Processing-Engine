package dev.dhruv.streaming.connectors.iceberg;

import java.io.Serializable;
import java.util.Map;

/** Converts a stream value to named fields in the destination table's current schema. */
@FunctionalInterface
public interface RecordMapper<T> extends Serializable {

    /** Maps one value to field names and values understood by the destination table. */
    Map<String, ?> map(T value);
}
