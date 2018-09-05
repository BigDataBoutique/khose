package com.bigdataboutique.khose.sinks;

import java.io.IOException;

/**
 * This sink allows writing data coming from a hose into Kafka
 *
 * TODO implement
 */
public class KafkaSink implements Sink {
    @Override
    public void write(Object obj) throws IOException {
        throw new UnsupportedOperationException("KafkaSink::write is not implemented");
    }

    @Override
    public void flush() {
        throw new UnsupportedOperationException("KafkaSink::flush is not implemented");
    }

    @Override
    public void close() throws IOException {
    }
}
