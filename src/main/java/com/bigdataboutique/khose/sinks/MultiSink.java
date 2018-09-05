package com.bigdataboutique.khose.sinks;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * This Sink allows for handling data by multiple sinks. It does not however guarantee
 * successful flush on all sinks.
 */
public final class MultiSink implements Sink {
    private List<Sink> sinks = new LinkedList<>();

    public void addSink(final Sink sink) {
        sinks.add(sink);
    }

    @Override
    public void write(Object obj) throws IOException {
        for (final Sink sink : sinks) {
            sink.write(obj);
        }
    }

    @Override
    public void flush() {
        for (final Sink sink : sinks) {
            sink.flush();
        }
    }

    @Override
    public void close() throws IOException {
        Exception exception = null;

        for (final Sink sink : sinks) {
            try {
                sink.close();
            } catch (Exception e) {
                if (exception == null) {
                    exception = e;
                    continue;
                }

                if (exception != e) {
                    exception.addSuppressed(e);
                }
            }
        }

        if (exception != null)
            throw new RuntimeException(exception);
    }
}
