package com.bigdataboutique.khose.sources;

import java.io.Closeable;

public interface Source extends Closeable {
    Runnable getWorker();
}
