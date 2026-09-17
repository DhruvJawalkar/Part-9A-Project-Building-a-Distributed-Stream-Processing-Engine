package dev.dhruv.streaming.metadata;

/**
 * Thrown when the metadata store cannot be read or written.
 *
 * <p>Unchecked, and that is a deliberate judgement rather than convenience. There is no useful
 * way for a caller to carry on without the store: a master that cannot record a state
 * transition must not perform it, because the entire value of writing first is that the record
 * exists if the master dies next. Making it checked would invite catch blocks that log and
 * continue, which is precisely the behaviour that loses jobs.
 */
public class MetadataStoreException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Creates the exception.
     *
     * @param message what failed
     * @param cause   the underlying failure
     */
    public MetadataStoreException(String message, Throwable cause) {
        super(message, cause);
    }
}
