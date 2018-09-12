package com.bigdataboutique.khose.sources;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.InitialPositionInStream;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.KinesisClientLibConfiguration;
import com.amazonaws.services.kinesis.clientlibrary.lib.worker.Worker;
import com.bigdataboutique.khose.config.KhoseConfigManager;
import com.bigdataboutique.khose.sinks.Sink;
import org.apache.parquet.Strings;
import org.joda.time.Period;
import org.joda.time.format.PeriodFormatter;
import org.joda.time.format.PeriodFormatterBuilder;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;

public class AwsKinesisSource implements Source {

    final Runnable worker;

    public AwsKinesisSource(final Sink sink) {
        // TODO take from config
        String workerId;
        try {
            workerId = InetAddress.getLocalHost().getCanonicalHostName() + ":" + UUID.randomUUID().toString();
        } catch (UnknownHostException e) {
            workerId = UUID.randomUUID().toString();
        }

        // Ensure the JVM will refresh the cached IP values of AWS resources (e.g. service endpoints).
        java.security.Security.setProperty("networkaddress.cache.ttl", "60");

        /*
         * The ProfileCredentialsProvider will return your [default]
         * credential profile by reading from the credentials file located at
         * (~/.aws/credentials).
         */
        AWSCredentialsProvider credentialsProvider = null;
        final String awsProfileName = KhoseConfigManager.get("source.kinesis.aws_profile");
        if (!Strings.isNullOrEmpty(awsProfileName)) {
            if ("aws_instance_profile".equals(awsProfileName)) {
                credentialsProvider = InstanceProfileCredentialsProvider.getInstance();
            } else {
                credentialsProvider = new ProfileCredentialsProvider(awsProfileName);
                try {
                    credentialsProvider.getCredentials();
                } catch (Exception e) {
                    throw new AmazonClientException("Cannot load the credentials from the credential profiles file. "
                            + "Please make sure that your credentials file is at the correct "
                            + "location (~/.aws/credentials), and is in valid format.", e);
                }
            }
        }

        if (credentialsProvider == null) {
            credentialsProvider = new DefaultAWSCredentialsProviderChain();
        }

        final KinesisClientLibConfiguration config = new KinesisClientLibConfiguration(
                "khose",
                KhoseConfigManager.get("source.kinesis.stream_name"),
                credentialsProvider,
                workerId);

        if (KhoseConfigManager.get("source.kinesis.initial_position") != null) {
            config.withInitialPositionInStream(InitialPositionInStream.valueOf(KhoseConfigManager.get("source.kinesis.initial_position").toUpperCase()));
        }
        if (KhoseConfigManager.get("source.kinesis.endpoint") != null) {
            config.withKinesisEndpoint(KhoseConfigManager.get("source.kinesis.endpoint"));
        }
        if (KhoseConfigManager.get("source.kinesis.region") != null) {
            config.withRegionName(KhoseConfigManager.get("source.kinesis.region"));
        }

        long _checkpointInterval = KhoseRecordProcessor.DEFAULT_CHECKPOINT_INTERVAL_MILLIS;
        if (KhoseConfigManager.get("source.kinesis.checkpoint_interval") != null) {
            // TODO more robust and flexible code for parsing interval strings
            PeriodFormatter formatter = new PeriodFormatterBuilder()
                    .appendMinutes().appendSuffix("m")
                    .toFormatter();

            Period p = formatter.parsePeriod(KhoseConfigManager.get("source.kinesis.checkpoint_interval"));
            _checkpointInterval = p.toStandardSeconds().getSeconds() * 1000;
        }
        final long checkpointInterval = _checkpointInterval;

        worker = new Worker.Builder()
                .recordProcessorFactory(() -> new KhoseRecordProcessor(sink, checkpointInterval))
                .config(config)
                .build();
    }

    public Runnable getWorker() {
        return worker;
    }

    @Override
    public void close() {
    }
}
