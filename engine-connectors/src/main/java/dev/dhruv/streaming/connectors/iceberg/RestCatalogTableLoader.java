package dev.dhruv.streaming.connectors.iceberg;

import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Serializable REST-catalog configuration for workers.
 *
 * <p>Only strings are sent with the job. Each worker creates its own REST client in
 * {@link #load()}, which avoids capturing a non-serializable catalog connection in a submitted
 * job. {@code properties} is also the right place for the REST catalog's S3 FileIO endpoint,
 * path-style access setting, and credentials provider configuration.
 */
record RestCatalogTableLoader(String catalogName, String catalogUri, String tableName,
        Map<String, String> properties) implements TableLoader {

    public RestCatalogTableLoader {
        requireText(catalogName, "catalogName");
        requireText(catalogUri, "catalogUri");
        requireText(tableName, "tableName");
        properties = Map.copyOf(Objects.requireNonNull(properties, "properties"));
    }

    @Override
    public Table load() {
        Map<String, String> configuration = new HashMap<>(properties);
        configuration.put(CatalogProperties.URI, catalogUri);
        RESTCatalog catalog = new RESTCatalog();
        catalog.initialize(catalogName, configuration);
        return catalog.loadTable(TableIdentifier.parse(tableName));
    }

    private static void requireText(String value, String name) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
