package com.mn.cdc.mysql;

import com.mn.cdc.exception.ConnectException;
import com.mn.cdc.structure.BaseRecord;
import com.mn.cdc.structure.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @program:cdc-master
 * @description 控制日志的读取
 * @author:miaoneng
 * @create:2021-09-08 14:09
 **/
public class ChainedReader implements Reader{
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final List<Reader> readers;
    private final String completionMessage;
    private final LinkedList<Reader> remainingReaders = new LinkedList<>();
    private final AtomicBoolean running = new AtomicBoolean();
    private final AtomicBoolean completed = new AtomicBoolean(true);
    private final AtomicReference<Reader> currentReader = new AtomicReference<>();
    private final AtomicReference<Runnable> uponCompletion = new AtomicReference<>();

    public static class Builder {

        private final List<Reader> readers = new ArrayList<>();
        private String completionMessage;

        public Builder addReader(Reader reader) {
            readers.add(reader);
            return this;
        }

        /**
         * Set the message that should be logged when all of the readers have completed their work.
         */
        public Builder completionMessage(String message) {
            this.completionMessage = message;
            return this;
        }

        public ChainedReader build() {
            return new ChainedReader(readers, completionMessage);
        }
    }

    /**
     * Create a new chained reader.
     */
    private ChainedReader(List<Reader> readers, String completionMessage) {
        this.readers = Collections.unmodifiableList(readers);
        this.completionMessage = completionMessage;

        for (Reader reader : this.readers) {
            reader.uponCompletion(this::readerCompletedPolling);
        }
    }

    @Override
    public void uponCompletion(Runnable handler) {
        uponCompletion.set(handler);
    }

    @Override
    public void initialize() {
        // initialize all of the readers ...
        readers.forEach(Reader::initialize);
    }

    @Override
    public void destroy() {
        // destroy all of the readers ...
        readers.forEach(Reader::destroy);
    }

    @Override
    public synchronized void start() {
        if (running.compareAndSet(false, true)) {
            completed.set(false);

            // Build up the list of readers that need to be called ...
            remainingReaders.clear();
            readers.forEach(remainingReaders::add);

            // Start the first reader, if there is one ...
            if (!startNextReader()) {
                // We couldn't start it ...
                running.set(false);
                completed.set(true);
            }
        }
    }

    @Override
    public synchronized void stop() {
        if (running.compareAndSet(true, false)) {
            // First, remove all readers that have not yet been started, which ensures the next one is is not started
            // while we're trying to stop the previous one ...
            remainingReaders.clear();
            // Then stop the currently-running reader but do not remove it as it will be removed when it completes ...
            Reader current = currentReader.get();
            if (current != null) {
                try {
                    logger.info("ChainedReader: Stopping the {} reader", current.name());
                    current.stop();
                } catch (Throwable t) {
                    logger.error("Unexpected error stopping the {} reader", current.name(), t);
                }
            }
        }
    }

    @Override
    public State state() {
        if (running.get()) {
            return State.RUNNING;
        }
        return completed.get() ? State.STOPPED : State.STOPPING;
    }

    @Override
    public List<BaseRecord> poll() throws InterruptedException, ConnectException {
        // We return no records when no reader is running. The caller thus gets back control
        // but must be able to handle such situation
        if (running.get() || !completed.get()) {
            final Reader reader = currentReader.get();
            if (reader != null) {
                return reader.poll();
            }
        }
        return null;
    }

    /**
     * Called when the previously-started reader has returned all of its records via {@link #poll() polling}.
     * Only when this method is called is the now-completed reader removed as the current reader, and this is what
     * guarantees that all records produced by the now-completed reader have been polled.
     */
    private synchronized void readerCompletedPolling() {
        if (!startNextReader()) {
            // We've finished with the last reader ...
            try {
                if (running.get() || !completed.get()) {
                    // Notify the handler ...
                    Runnable handler = uponCompletion.get();
                    if (handler != null) {
                        handler.run();
                    }
                    // and output our message ...
                    if (completionMessage != null) {
                        logger.info(completionMessage);
                    }
                }
            } finally {
                // And since this is the last reader, make sure this chain is also stopped ...
                completed.set(true);
                running.set(false);
            }
        }
    }

    /**
     * Start the next reader.
     *
     * @return {@code true} if the next reader was started, or {@code false} if there are no more readers
     */
    private boolean startNextReader() {
        Reader reader = remainingReaders.isEmpty() ? null : remainingReaders.pop();
        if (reader == null) {
            // There are no readers, so nothing to do ...
            Reader lastReader = currentReader.getAndSet(null);
            if (lastReader != null) {
                // Make sure it has indeed stopped ...
                lastReader.stop();
            }
            return false;
        }

        // There is at least one more reader, so start it ...
        Reader lastReader = currentReader.getAndSet(null);
        if (lastReader != null) {
            logger.info("Transitioning from the {} reader to the {} reader", lastReader.name(), reader.name());
        } else {
            logger.info("启动{}", reader.name());
        }
        reader.start();
        currentReader.set(reader);
        return true;
    }

    @Override
    public String name() {
        Reader reader = currentReader.get();
        return reader != null ? reader.name() : "chained";
    }

}
