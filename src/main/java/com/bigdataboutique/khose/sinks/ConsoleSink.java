package com.bigdataboutique.khose.sinks;

import java.io.IOException;

public class ConsoleSink implements Sink {
    @Override
    public void write(Object obj) throws IOException {
        System.out.println(obj);
    }

    @Override
    public void flush() {
    }

    @Override
    public void close() throws IOException {
    }
}
