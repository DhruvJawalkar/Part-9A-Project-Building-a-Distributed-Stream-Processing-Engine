package dev.dhruv.streaming.connectors.iceberg;

import org.apache.iceberg.Table;

import java.io.Serializable;

/**
 * Reopens the destination table on the worker after an {@code IcebergSink} has been shipped.
 *
 * <p>A catalog client is a runtime resource, not job configuration, so it deliberately lives
 * behind this small serializable factory rather than in the sink itself.
 */
@FunctionalInterface
interface TableLoader extends Serializable {

    /** Opens the table to which this sink writes. */
    Table load() throws Exception;
}
