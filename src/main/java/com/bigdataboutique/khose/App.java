package com.bigdataboutique.khose;

import com.bigdataboutique.khose.config.KhoseConfigManager;
import com.bigdataboutique.khose.sinks.ConsoleSink;
import com.bigdataboutique.khose.sinks.JsonlinesSink;
import com.bigdataboutique.khose.sinks.ParquetFilesSink;
import com.bigdataboutique.khose.sinks.Sink;
import com.bigdataboutique.khose.sources.AwsKinesisSource;
import com.bigdataboutique.khose.sources.Source;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.shaded.com.google.common.base.Objects;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.util.UUID;

public class App {

    private static final Log LOG = LogFactory.getLog(App.class);

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

        final Sink sink;
        final String outputFormat = KhoseConfigManager.getOrDefault("sink.s3.format", "parquet");
        if ("parquet".equals(outputFormat)) {
            final String outputPath = s3Bucket + s3Prefix;
            sink = new ParquetFilesSink(schema,
                    KhoseConfigManager.getOrDefault("sink.s3.compression", "gzip"), outputPath);
        } else if ("jsonlines".equals(outputFormat)) {
            final String outputPath = Objects.firstNonNull(KhoseConfigManager.get("sink.s3.path"), KhoseConfigManager.get("sink.s3.bucket"));
            sink = new JsonlinesSink(KhoseConfigManager.get("sink.s3.prefix"), "gzip", outputPath);
        } else {
            throw new RuntimeException("Unsupported output format " + outputFormat);
        }

        // TODO create the correct source per hose correctly from config
        final Source source = new AwsKinesisSource(sink);

        LOG.info(String.format("Initializing Khose with source %s and sink %s", source.toString(), sink.toString()));

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
