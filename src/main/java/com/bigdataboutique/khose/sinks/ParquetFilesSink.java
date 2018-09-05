package com.bigdataboutique.khose.sinks;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.codehaus.jackson.JsonNode;

import java.io.IOException;
import java.util.UUID;

import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_WRITER_VERSION;

public class ParquetFilesSink implements AutoCloseable, Sink {

    private static final Log LOG = LogFactory.getLog(ParquetFilesSink.class);

    private final String uuid = UUID.randomUUID().toString().replace("-", "");
    private int part = 0;

    private final MessageType schema;
    private final Path outputPath;
    private final Configuration conf;
    private ParquetWriter<Group> writer;
    private final SimpleGroupFactory sfg;

    private final int blockSize;
    private final int pageSize;
    private final WriteSupport<Group> writeSupport;
    private CompressionCodecName codec;

    public ParquetFilesSink(final MessageType schema, final String compression, final String outputPath) throws IOException {
        this.codec = CompressionCodecName.fromConf(compression);
        this.blockSize = 256 * 1024 * 1024;
        this.pageSize = 1 * 1024 * 1024;

        this.conf = new Configuration(true);
        GroupWriteSupport.setSchema(schema, conf);
        this.writeSupport = new GroupWriteSupport();
        this.schema = schema;
        this.sfg = new SimpleGroupFactory(this.schema);

        this.outputPath = new Path(outputPath);
        this.writer = newWriter();
    }

    private ParquetWriter<Group> newWriter() throws IOException {
        return new ParquetWriter(new Path(this.outputPath, generateFileName()),
                writeSupport, codec, blockSize, pageSize, pageSize,
                DEFAULT_IS_DICTIONARY_ENABLED, DEFAULT_IS_VALIDATING_ENABLED, DEFAULT_WRITER_VERSION,
                conf
        );
    }

    private String generateFileName() {
        // TODO add dates and support formating via config
        return String.format("part-%s-%06d.%s.parquet", uuid, part++,
                codec.getParquetCompressionCodec().name().toLowerCase());
    }

    public void write(final Object obj) throws IOException {
        write((JsonNode) obj);
    }

    public void write(final JsonNode obj) throws IOException {
        final Group group = sfg.newGroup();
        for (final ColumnDescriptor c : schema.getColumns()) {
            final String fieldName = c.getPath()[c.getPath().length - 1];
            appendToGroup(group, fieldName, obj.get(fieldName), c.getPrimitiveType());
        }
        writer.write(group);
    }

    public void flush() {
        if (writer != null) {
            try {
                writer.close();
            } catch (IOException e) {
                LOG.error("Error while flushing Parquet file", e);
                return;
            }
        }

        try {
            LOG.info("Flushing Parquet file");
            this.writer = newWriter();
        } catch (IOException e) {
            LOG.error("Error while trying to create ParquetWriter", e);
            writer  = null;
        }
    }

    private void appendToGroup(Group group, String fieldName, JsonNode jsonNode, PrimitiveType primitiveType) throws IOException {
        switch (primitiveType.getPrimitiveTypeName()) {
            case INT64:
                // TODO handle ISO dates (convert to millis)
                group.append(fieldName, jsonNode.asLong());
                return;
            case DOUBLE:
            case FLOAT:
                group.append(fieldName, jsonNode.asDouble());
                return;
            case INT32:
                group.append(fieldName, jsonNode.asInt());
                return;
            case BOOLEAN:
                group.append(fieldName, jsonNode.asBoolean());
                return;
// TODO
//            case BINARY:
//                group.append(fieldName, new Binary(jsonNode.getBinaryValue()));
            default:
                if (jsonNode == null) {
                    group.append(fieldName, Binary.EMPTY); // TODO is treating null as Binary.EMPTY correct?
                } else {
                    group.append(fieldName, jsonNode.asText());
                }
        }
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }
}
