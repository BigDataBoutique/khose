package com.bigdataboutique.khose.sinks;

import java.io.Closeable;
import java.io.IOException;

public interface Sink extends Closeable {
    void write(Object obj) throws IOException;
    void flush();
}
