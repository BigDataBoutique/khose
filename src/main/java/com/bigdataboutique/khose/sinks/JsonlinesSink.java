package com.bigdataboutique.khose.sinks;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.bigdataboutique.khose.config.KhoseConfigManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.codehaus.jackson.JsonNode;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Files;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

public class JsonlinesSink implements Sink {
    private static final Log LOG = LogFactory.getLog(JsonlinesSink.class);

    private final Object writeLock = new Object();

    private final String uuid = UUID.randomUUID().toString().replace("-", "");
    private final String compression;
    private int part = 0;

    private final String s3Bucket;
    private final String outputPath;
    private final String prefixPattern;

    private FileOutputStream output;
    private Writer writer;
    private File currentFile;

    public JsonlinesSink(final String prefixPattern, final String compression, final String outputPath) throws IOException {
        if (!"gzip".equals(compression)) {
            throw new IllegalArgumentException("Only gzip compression is supported at the moment for jsonlines sink");
        }

        if (prefixPattern != null && !prefixPattern.endsWith("/")) {
            this.prefixPattern = prefixPattern + "/";
        } else {
            this.prefixPattern = prefixPattern;
        }
        this.compression = compression;

        if (outputPath.startsWith("s3")) {
            this.outputPath = Files.createTempDirectory("khose-").toAbsolutePath().toString();
            this.s3Bucket = outputPath.substring(outputPath.indexOf("://")+3);
        } else {
            this.outputPath = outputPath;
            this.s3Bucket = null;
        }

        createNewWriter();
    }

    private void createNewWriter() throws IOException {
        this.currentFile = new File(outputPath, generateFileName());
        this.output = new FileOutputStream(currentFile, false);
        this.writer = new OutputStreamWriter(new GZIPOutputStream(output, true), "UTF-8");
    }

    private String generateFileName() {
        // TODO add dates and support formatting via config
        final String fileName = String.format("part-%s-%06d.jsonl.%s", uuid, part++, "gz");
        // TODO prefixPattern
        return fileName;
    }

    @Override
    public void write(final Object obj) throws IOException {
        write((JsonNode) obj);
    }

    private void write(final JsonNode obj) throws IOException {
        synchronized (writeLock) {
            writer.write(obj.toString());
            writer.write("\n");
        }
    }

    @Override
    public void flush() {
        File fileToUpload = currentFile;
        synchronized (writeLock) {
            if (writer != null) {
                try {
                    writer.flush();
                    writer.close();
                    writer = null;
                } catch (IOException e) {
                    LOG.error("Error while flushing file", e);
                    return;
                }
            }

            if (output != null) {
                try {
                    output.close();
                    output = null;
                } catch (IOException e) {
                    LOG.error("Error while flushing file", e);
                    return;
                }
            }

            try {
                createNewWriter();
            } catch (IOException e) {
                LOG.error("Error while flushing to file", e);
            }
        }

        if (s3Bucket != null) {
            uploadS3(fileToUpload, s3Bucket, prefixPattern + fileToUpload.getName());
            fileToUpload.deleteOnExit();
        }
    }

    private boolean uploadS3(final File file, final String bucketName, final String objectName) {
        try {
            final AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion(KhoseConfigManager.getOrDefault("sink.s3.region", "us-east-1"))
                    //.withCredentials(new ProfileCredentialsProvider()) // TODO
                    .withCredentials(new DefaultAWSCredentialsProviderChain())
                    .build();

            final ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType("application/json");
            final PutObjectRequest request = new PutObjectRequest(bucketName, objectName, file);
            request.withMetadata(metadata);
            final PutObjectResult r = s3Client.putObject(request);
            return true;
        }
        catch(AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            LOG.error("Amazon S3 returned an error response", e);
            return false;
        }
        catch(SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            LOG.error("Amazon S3 is unreachable", e);
            return false;
        }
    }

    @Override
    public void close() throws IOException {
        writer.close();
        output.close();
    }
}
