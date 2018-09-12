# Khose

***Khose*** is a small and efficient utility to get data from Kafka/Kinesis streams hose to immutable data at rest somewhere somehow.

***Khose*** supports multiple _hoses_, each with a single input _source_ and one or more output _sinks_.

***Khose*** is meant to run continously over Kafka/Kinesis data streams and create immutable data from it - e.g. Parquet files on S3 or HDFS.

## Compiling

We use Gradle as our build system. To compile simply run `gradle build`. To pack a jar you can run and/or deploy, execute `gradle shadowJar`.

### Configuring

Khose expects a single yaml config file, in which you can define one or more _hose_ configuration. One data _hose_ should define exactly one input _source_ (e.g. AWS Kinesis stream and it's configurations), and one or more output _sinks_ (e.g. print to console and write to S3 in Parquet format).

An example for a valid configuration file:

```yaml
hoses:
  default:
    source:
      kinesis:
        endpoint: kinesis.us-west-2.amazonaws.com
        region: us-west-2
        stream_name: production-events
        initial_position: latest
        aws_profile: my-profile
    sink:
      console:
        enabled: true
      s3:
        bucket: s3a://my-parquet-bucket/
        prefix: events-stream-test/
        format: parquet
        compression: gzip
        schema_file: events.parquet.schema
```

## Running

Run via `java -jar khose.jar config.yml` where config.yml is a configuration file with the format detailed in the docs.
