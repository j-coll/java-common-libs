package org.opencb.commons.io;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.xerial.snappy.SnappyInputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.zip.GZIPInputStream;
/**
 * Created by jacobo on 25/02/15.
 */
public class StringDataReader implements DataReader<String> {

    protected BufferedReader reader;
    protected final Path path;
    protected static Logger logger = LoggerFactory.getLogger(StringDataReader.class);
    protected long readLines = 0L;
    protected int lastAvailable = 0;
    private FileInputStream fileInputStream;
    private Consumer<Integer> readBytesListener;

    public StringDataReader(Path path) {
        this.path = path;
    }

    @Override
    public boolean open() {
        try {
            String fileName = path.toFile().getName();
            fileInputStream = new FileInputStream(path.toFile());
            lastAvailable = fileInputStream.available();
            if (fileName.endsWith(".gz")) {
                logger.debug("Gzip input compress");
                this.reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(fileInputStream)));
//            } else if (fileName.endsWith(".snappy") || fileName.endsWith(".snz")) {
//                logger.info("Snappy input compress");
//                this.reader = new BufferedReader(new InputStreamReader(new SnappyInputStream(new FileInputStream(path.toFile()))));
            } else {
                logger.debug("Plain input compress");
//                this.reader = Files.newBufferedReader(path, Charset.defaultCharset());
                this.reader = new BufferedReader(new InputStreamReader(fileInputStream));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }

    @Override
    public boolean close() {
        try {
            reader.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return true;
    }

    @Override
    public List<String> read() {
        try {
            String line = reader.readLine();
            if (line == null) {
                return Collections.emptyList();
            } else {
                updateReadBytes();
                return Collections.singletonList(line);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public List<String> read(int batchSize) {
        List<String> batch = new ArrayList<>(batchSize);
        try {
            for (int i = 0; i < batchSize; i++) {
                String line = reader.readLine();
                if (line == null) {
                    return batch;
                }
                batch.add(line);
            }
            updateReadBytes();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return batch;
    }

    private void updateReadBytes() throws IOException {
        int newAvailable = fileInputStream.available();
        if (readBytesListener != null) {
            readBytesListener.accept(lastAvailable - newAvailable);
        }
        lastAvailable = newAvailable;
        this.readLines += readLines;
    }

    public void setReadBytesListener(Consumer<Integer> onReadBytes) {
        this.readBytesListener = onReadBytes;
    }

    public long getFileSize() throws IOException {
        return Files.size(path);
    }

}
