package com.bigdataboutique.khose;

import com.bigdataboutique.khose.config.KhoseConfigManager;
import com.bigdataboutique.khose.sinks.ParquetFilesSink;
import com.bigdataboutique.khose.sources.AwsKinesisSource;
import com.bigdataboutique.khose.sources.Source;
import org.apache.commons.io.FileUtils;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;

public class App {

    public static void main(String[] args) throws IOException {
        String configFile = "config.yml";
        if (args.length > 0) {
            configFile = args[0];
        }
        KhoseConfigManager.initialize(configFile);

        // TODO support running multiple hoses via threads

        // TODO move to config
        final String workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID();

        // We support multiple types of sinks, but at the moment they are not being loaded automaticaly via config
        // TODO create the correct sink per hose and load the sink correctly
        final String schemaString = FileUtils.readFileToString(new File(KhoseConfigManager.get("sink.s3.schema_file")), "UTF-8");
        final MessageType schema = MessageTypeParser.parseMessageType(schemaString);

        // TODO outputPath can either be a filesystem path, or an S3 bucket + prefix; parse configs correctly to decide
        final String s3Prefix = KhoseConfigManager.get("sink.s3.prefix");
        final String s3Bucket = KhoseConfigManager.get("sink.s3.bucket");
        final String outputPath = s3Bucket + s3Prefix;

        final String outputFormat = KhoseConfigManager.getOrDefault("sink.s3.format", "parquet");
        if (!"parquet".equals(outputFormat)) {
            throw new RuntimeException("Only Parquet format is supported for output");
        }
        final ParquetFilesSink sink = new ParquetFilesSink(schema,
                KhoseConfigManager.getOrDefault("sink.s3.compression", "gzip"), outputPath);

        // TODO create the correct source per hose correctly from config
        final Source source = new AwsKinesisSource(sink);

        // TODO run this better, support multiple hoses on threads, and clean shutdown on interrupts
        int exitCode = 0;
        try {
            source.getWorker().run();
        } catch (Throwable t) {
            System.err.println("Caught throwable while processing data.");
            t.printStackTrace();
            exitCode = 1;
        } finally {
            try {
                sink.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        System.exit(exitCode);
    }
}
