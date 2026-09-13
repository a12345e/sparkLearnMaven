package com.example.ontologyprocessor.io;

/**
 * A storage backend failed.
 *
 * <p>Unchecked on purpose. Every contract in this package is meant to be
 * implemented by a lambda or a small class and called from inside a Spark
 * pipeline, and a checked exception there would either be swallowed at each
 * call site or forced into the signature of every stage that touches IO.
 *
 * <p>Implementations should wrap whatever their client throws - an
 * {@code IOException} from HDFS, a REST failure from Elasticsearch - in one of
 * these, with a message naming the table and the backend, so a caller can read
 * what went wrong without knowing which backend it was talking to.
 */
public class IoException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public IoException(String message) {
        super(message);
    }

    public IoException(String message, Throwable cause) {
        super(message, cause);
    }
}
