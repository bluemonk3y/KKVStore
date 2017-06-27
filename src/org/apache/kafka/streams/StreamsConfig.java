//
// Temp Hack to try and override Assigned to support variable distribution model
//

package org.apache.kafka.streams;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.ValidString;
import org.apache.kafka.common.metrics.Sensor.RecordingLevel;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes.ByteArraySerde;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.processor.DefaultPartitionGrouper;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.internals.StreamPartitionAssignor;
import org.apache.kafka.streams.processor.internals.StreamThread;

public class StreamsConfig extends AbstractConfig {
    private static final ConfigDef CONFIG;
    public static final String CONSUMER_PREFIX = "consumer.";
    public static final String PRODUCER_PREFIX = "producer.";
    public static final String STATE_DIR_CONFIG = "state.dir";
    private static final String STATE_DIR_DOC = "Directory location for state store.";
    /** @deprecated */
    @Deprecated
    public static final String ZOOKEEPER_CONNECT_CONFIG = "zookeeper.connect";
    private static final String ZOOKEEPER_CONNECT_DOC = "Zookeeper connect string for Kafka topics management.";
    public static final String COMMIT_INTERVAL_MS_CONFIG = "commit.interval.ms";
    private static final String COMMIT_INTERVAL_MS_DOC = "The frequency with which to save the position of the processor.";
    public static final String POLL_MS_CONFIG = "poll.ms";
    private static final String POLL_MS_DOC = "The amount of time in milliseconds to block waiting for input.";
    public static final String NUM_STREAM_THREADS_CONFIG = "num.stream.threads";
    private static final String NUM_STREAM_THREADS_DOC = "The number of threads to execute stream processing.";
    public static final String NUM_STANDBY_REPLICAS_CONFIG = "num.standby.replicas";
    private static final String NUM_STANDBY_REPLICAS_DOC = "The number of standby replicas for each task.";
    public static final String BUFFERED_RECORDS_PER_PARTITION_CONFIG = "buffered.records.per.partition";
    private static final String BUFFERED_RECORDS_PER_PARTITION_DOC = "The maximum number of records to buffer per partition.";
    public static final String STATE_CLEANUP_DELAY_MS_CONFIG = "state.cleanup.delay.ms";
    private static final String STATE_CLEANUP_DELAY_MS_DOC = "The amount of time in milliseconds to wait before deleting state when a partition has migrated.";
    public static final String TIMESTAMP_EXTRACTOR_CLASS_CONFIG = "timestamp.extractor";
    private static final String TIMESTAMP_EXTRACTOR_CLASS_DOC = "Timestamp extractor class that implements the <code>TimestampExtractor</code> interface.";
    public static final String PARTITION_GROUPER_CLASS_CONFIG = "partition.grouper";
    private static final String PARTITION_GROUPER_CLASS_DOC = "Partition grouper class that implements the <code>PartitionGrouper</code> interface.";
    public static final String APPLICATION_ID_CONFIG = "application.id";
    private static final String APPLICATION_ID_DOC = "An identifier for the stream processing application. Must be unique within the Kafka cluster. It is used as 1) the default client-id prefix, 2) the group-id for membership management, 3) the changelog topic prefix.";
    public static final String REPLICATION_FACTOR_CONFIG = "replication.factor";
    private static final String REPLICATION_FACTOR_DOC = "The replication factor for change log topics and repartition topics created by the stream processing application.";
    public static final String KEY_SERDE_CLASS_CONFIG = "key.serde";
    private static final String KEY_SERDE_CLASS_DOC = "Serializer / deserializer class for key that implements the <code>Serde</code> interface.";
    public static final String VALUE_SERDE_CLASS_CONFIG = "value.serde";
    private static final String VALUE_SERDE_CLASS_DOC = "Serializer / deserializer class for value that implements the <code>Serde</code> interface.";
    public static final String APPLICATION_SERVER_CONFIG = "application.server";
    private static final String APPLICATION_SERVER_DOC = "A host:port pair pointing to an embedded user defined endpoint that can be used for discovering the locations of state stores within a single KafkaStreams application";
    public static final String METRICS_SAMPLE_WINDOW_MS_CONFIG = "metrics.sample.window.ms";
    public static final String METRICS_NUM_SAMPLES_CONFIG = "metrics.num.samples";
    public static final String METRICS_RECORDING_LEVEL_CONFIG = "metrics.recording.level";
    public static final String METRIC_REPORTER_CLASSES_CONFIG = "metric.reporters";
    public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
    public static final String CLIENT_ID_CONFIG = "client.id";
    public static final String ROCKSDB_CONFIG_SETTER_CLASS_CONFIG = "rocksdb.config.setter";
    private static final String ROCKSDB_CONFIG_SETTER_CLASS_DOC = "A Rocks DB config setter class that implements the <code>RocksDBConfigSetter</code> interface";
    public static final String WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG = "windowstore.changelog.additional.retention.ms";
    private static final String WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_DOC = "Added to a windows maintainMs to ensure data is not deleted from the log prematurely. Allows for clock drift. Default is 1 day";
    public static final String CACHE_MAX_BYTES_BUFFERING_CONFIG = "cache.max.bytes.buffering";
    private static final String CACHE_MAX_BYTES_BUFFERING_DOC = "Maximum number of memory bytes to be used for buffering across all threads";
    public static final String SECURITY_PROTOCOL_CONFIG = "security.protocol";
    private static final String SECURITY_PROTOCOL_DOC;
    public static final String DEFAULT_SECURITY_PROTOCOL = "PLAINTEXT";
    public static final String CONNECTIONS_MAX_IDLE_MS_CONFIG = "connections.max.idle.ms";
    private static final String CONNECTIONS_MAX_IDLE_MS_DOC = "Close idle connections after the number of milliseconds specified by this config.";
    public static final String RETRY_BACKOFF_MS_CONFIG = "retry.backoff.ms";
    private static final String RETRY_BACKOFF_MS_DOC = "The amount of time to wait before attempting to retry a failed request to a given topic partition. This avoids repeatedly sending requests in a tight loop under some failure scenarios.";
    public static final String METADATA_MAX_AGE_CONFIG = "metadata.max.age.ms";
    private static final String METADATA_MAX_AGE_DOC = "The period of time in milliseconds after which we force a refresh of metadata even if we haven\'t seen any partition leadership changes to proactively discover any new brokers or partitions.";
    public static final String RECONNECT_BACKOFF_MS_CONFIG = "reconnect.backoff.ms";
    private static final String RECONNECT_BACKOFF_MS_DOC = "The amount of time to wait before attempting to reconnect to a given host. This avoids repeatedly connecting to a host in a tight loop. This backoff applies to all requests sent by the consumer to the broker.";
    public static final String SEND_BUFFER_CONFIG = "send.buffer.bytes";
    private static final String SEND_BUFFER_DOC = "The size of the TCP send buffer (SO_SNDBUF) to use when sending data. If the value is -1, the OS default will be used.";
    public static final String RECEIVE_BUFFER_CONFIG = "receive.buffer.bytes";
    private static final String RECEIVE_BUFFER_DOC = "The size of the TCP receive buffer (SO_RCVBUF) to use when reading data. If the value is -1, the OS default will be used.";
    public static final String REQUEST_TIMEOUT_MS_CONFIG = "request.timeout.ms";
    private static final String REQUEST_TIMEOUT_MS_DOC = "The configuration controls the maximum amount of time the client will wait for the response of a request. If the response is not received before the timeout elapses the client will resend the request if necessary or fail the request if retries are exhausted.";
    private static final Map<String, Object> PRODUCER_DEFAULT_OVERRIDES;
    private static final Map<String, Object> CONSUMER_DEFAULT_OVERRIDES;

    public static String consumerPrefix(String consumerProp) {
        return "consumer." + consumerProp;
    }

    public static String producerPrefix(String producerProp) {
        return "producer." + producerProp;
    }

    public static ConfigDef configDef() {
        return new ConfigDef(CONFIG);
    }

    public StreamsConfig(Map<?, ?> props) {
        super(CONFIG, props);
    }

    private Map<String, Object> getCommonConsumerConfigs() throws ConfigException {
        Map clientProvidedProps = this.getClientPropsWithPrefix("consumer.", ConsumerConfig.configNames());
        if(clientProvidedProps.containsKey("enable.auto.commit")) {
            throw new ConfigException("Unexpected user-specified consumer config enable.auto.commit, as the streams client will always turn off auto committing.");
        } else {
            HashMap consumerProps = new HashMap(CONSUMER_DEFAULT_OVERRIDES);
            consumerProps.putAll(clientProvidedProps);
            consumerProps.put("bootstrap.servers", this.originals().get("bootstrap.servers"));
            consumerProps.remove("zookeeper.connect");
            return consumerProps;
        }
    }

    public Map<String, Object> getConsumerConfigs(StreamThread streamThread, String groupId, String clientId) throws ConfigException {
        Map consumerProps = this.getCommonConsumerConfigs();
        consumerProps.put("group.id", groupId);
        consumerProps.put("client.id", clientId + "-consumer");
        consumerProps.put("__stream.thread.instance__", streamThread);
        consumerProps.put("replication.factor", this.getInt("replication.factor"));
        consumerProps.put("num.standby.replicas", this.getInt("num.standby.replicas"));
//        consumerProps.put("partition.assignment.strategy", StreamPartitionAssignor.class.getName());

/**
 * @implNote trying to understand the consequences of changing the StreamPartitionArrigner to RRAssigner - she go bang atm.
 */
        consumerProps.put("partition.assignment.strategy", RoundRobinAssignor.class.getName());
        consumerProps.put("windowstore.changelog.additional.retention.ms", this.getLong("windowstore.changelog.additional.retention.ms"));
        consumerProps.put("application.server", this.getString("application.server"));
        return consumerProps;
    }

    public Map<String, Object> getRestoreConsumerConfigs(String clientId) throws ConfigException {
        Map consumerProps = this.getCommonConsumerConfigs();
        consumerProps.remove("group.id");
        consumerProps.put("client.id", clientId + "-restore-consumer");
        return consumerProps;
    }

    public Map<String, Object> getProducerConfigs(String clientId) {
        HashMap props = new HashMap(PRODUCER_DEFAULT_OVERRIDES);
        props.putAll(this.getClientPropsWithPrefix("producer.", ProducerConfig.configNames()));
        props.put("bootstrap.servers", this.originals().get("bootstrap.servers"));
        props.put("client.id", clientId + "-producer");
        return props;
    }

    private Map<String, Object> getClientPropsWithPrefix(String prefix, Set<String> configNames) {
        Map props = this.clientProps(configNames, this.originals());
        props.putAll(this.originalsWithPrefix(prefix));
        return props;
    }

    public Serde keySerde() {
        try {
            Serde e = (Serde)this.getConfiguredInstance("key.serde", Serde.class);
            e.configure(this.originals(), true);
            return e;
        } catch (Exception var2) {
            throw new StreamsException(String.format("Failed to configure key serde %s", new Object[]{this.get("key.serde")}), var2);
        }
    }

    public Serde valueSerde() {
        try {
            Serde e = (Serde)this.getConfiguredInstance("value.serde", Serde.class);
            e.configure(this.originals(), false);
            return e;
        } catch (Exception var2) {
            throw new StreamsException(String.format("Failed to configure value serde %s", new Object[]{this.get("value.serde")}), var2);
        }
    }

    private Map<String, Object> clientProps(Set<String> configNames, Map<String, Object> originals) {
        HashMap parsed = new HashMap();
        Iterator i$ = configNames.iterator();

        while(i$.hasNext()) {
            String configName = (String)i$.next();
            if(originals.containsKey(configName)) {
                parsed.put(configName, originals.get(configName));
            }
        }

        return parsed;
    }

    public static void main(String[] args) {
        System.out.println(CONFIG.toHtmlTable());
    }

    static {
        SECURITY_PROTOCOL_DOC = CommonClientConfigs.SECURITY_PROTOCOL_DOC;
        CONFIG = (new ConfigDef()).define("application.id", Type.STRING, Importance.HIGH, "An identifier for the stream processing application. Must be unique within the Kafka cluster. It is used as 1) the default client-id prefix, 2) the group-id for membership management, 3) the changelog topic prefix.").define("bootstrap.servers", Type.LIST, Importance.HIGH, "A list of host/port pairs to use for establishing the initial connection to the Kafka cluster. The client will make use of all servers irrespective of which servers are specified here for bootstrapping&mdash;this list only impacts the initial hosts used to discover the full set of servers. This list should be in the form <code>host1:port1,host2:port2,...</code>. Since these servers are just used for the initial connection to discover the full cluster membership (which may change dynamically), this list need not contain the full set of servers (you may want more than one, though, in case a server is down).").define("client.id", Type.STRING, "", Importance.HIGH, "An id string to pass to the server when making requests. The purpose of this is to be able to track the source of requests beyond just ip/port by allowing a logical application name to be included in server-side request logging.").define("zookeeper.connect", Type.STRING, "", Importance.HIGH, "Zookeeper connect string for Kafka topics management.").define("state.dir", Type.STRING, "/tmp/kafka-streams", Importance.MEDIUM, "Directory location for state store.").define("replication.factor", Type.INT, Integer.valueOf(1), Importance.MEDIUM, "The replication factor for change log topics and repartition topics created by the stream processing application.").define("timestamp.extractor", Type.CLASS, FailOnInvalidTimestamp.class.getName(), Importance.MEDIUM, "Timestamp extractor class that implements the <code>TimestampExtractor</code> interface.").define("partition.grouper", Type.CLASS, DefaultPartitionGrouper.class.getName(), Importance.MEDIUM, "Partition grouper class that implements the <code>PartitionGrouper</code> interface.").define("key.serde", Type.CLASS, ByteArraySerde.class.getName(), Importance.MEDIUM, "Serializer / deserializer class for key that implements the <code>Serde</code> interface.").define("value.serde", Type.CLASS, ByteArraySerde.class.getName(), Importance.MEDIUM, "Serializer / deserializer class for value that implements the <code>Serde</code> interface.").define("commit.interval.ms", Type.LONG, Integer.valueOf(30000), Importance.LOW, "The frequency with which to save the position of the processor.").define("poll.ms", Type.LONG, Integer.valueOf(100), Importance.LOW, "The amount of time in milliseconds to block waiting for input.").define("num.stream.threads", Type.INT, Integer.valueOf(1), Importance.LOW, "The number of threads to execute stream processing.").define("num.standby.replicas", Type.INT, Integer.valueOf(0), Importance.LOW, "The number of standby replicas for each task.").define("buffered.records.per.partition", Type.INT, Integer.valueOf(1000), Importance.LOW, "The maximum number of records to buffer per partition.").define("state.cleanup.delay.ms", Type.LONG, Integer.valueOf('\uea60'), Importance.LOW, "The amount of time in milliseconds to wait before deleting state when a partition has migrated.").define("metric.reporters", Type.LIST, "", Importance.LOW, "A list of classes to use as metrics reporters. Implementing the <code>MetricReporter</code> interface allows plugging in classes that will be notified of new metric creation. The JmxReporter is always included to register JMX statistics.").define("metrics.sample.window.ms", Type.LONG, Integer.valueOf(30000), Range.atLeast(Integer.valueOf(0)), Importance.LOW, "The window of time a metrics sample is computed over.").define("metrics.num.samples", Type.INT, Integer.valueOf(2), Range.atLeast(Integer.valueOf(1)), Importance.LOW, "The number of samples maintained to compute metrics.").define("metrics.recording.level", Type.STRING, RecordingLevel.INFO.toString(), ValidString.in(new String[]{RecordingLevel.INFO.toString(), RecordingLevel.DEBUG.toString()}), Importance.LOW, "The highest recording level for metrics.").define("application.server", Type.STRING, "", Importance.LOW, "A host:port pair pointing to an embedded user defined endpoint that can be used for discovering the locations of state stores within a single KafkaStreams application").define("rocksdb.config.setter", Type.CLASS, (Object)null, Importance.LOW, "A Rocks DB config setter class that implements the <code>RocksDBConfigSetter</code> interface").define("windowstore.changelog.additional.retention.ms", Type.LONG, Integer.valueOf(86400000), Importance.MEDIUM, "Added to a windows maintainMs to ensure data is not deleted from the log prematurely. Allows for clock drift. Default is 1 day").define("cache.max.bytes.buffering", Type.LONG, Long.valueOf(10485760L), Range.atLeast(Integer.valueOf(0)), Importance.LOW, "Maximum number of memory bytes to be used for buffering across all threads").define("security.protocol", Type.STRING, "PLAINTEXT", Importance.MEDIUM, SECURITY_PROTOCOL_DOC).define("connections.max.idle.ms", Type.LONG, Integer.valueOf(540000), Importance.MEDIUM, "Close idle connections after the number of milliseconds specified by this config.").define("retry.backoff.ms", Type.LONG, Long.valueOf(100L), Range.atLeast(Long.valueOf(0L)), Importance.LOW, "The amount of time to wait before attempting to retry a failed request to a given topic partition. This avoids repeatedly sending requests in a tight loop under some failure scenarios.").define("metadata.max.age.ms", Type.LONG, Integer.valueOf(300000), Range.atLeast(Integer.valueOf(0)), Importance.LOW, "The period of time in milliseconds after which we force a refresh of metadata even if we haven\'t seen any partition leadership changes to proactively discover any new brokers or partitions.").define("reconnect.backoff.ms", Type.LONG, Long.valueOf(50L), Range.atLeast(Long.valueOf(0L)), Importance.LOW, "The amount of time to wait before attempting to reconnect to a given host. This avoids repeatedly connecting to a host in a tight loop. This backoff applies to all requests sent by the consumer to the broker.").define("send.buffer.bytes", Type.INT, Integer.valueOf(131072), Range.atLeast(Integer.valueOf(0)), Importance.MEDIUM, "The size of the TCP send buffer (SO_SNDBUF) to use when sending data. If the value is -1, the OS default will be used.").define("receive.buffer.bytes", Type.INT, Integer.valueOf('耀'), Range.atLeast(Integer.valueOf(0)), Importance.MEDIUM, "The size of the TCP receive buffer (SO_RCVBUF) to use when reading data. If the value is -1, the OS default will be used.").define("request.timeout.ms", Type.INT, Integer.valueOf('鱀'), Range.atLeast(Integer.valueOf(0)), Importance.MEDIUM, "The configuration controls the maximum amount of time the client will wait for the response of a request. If the response is not received before the timeout elapses the client will resend the request if necessary or fail the request if retries are exhausted.");
        HashMap tempConsumerDefaultOverrides = new HashMap();
        tempConsumerDefaultOverrides.put("linger.ms", "100");
        tempConsumerDefaultOverrides.put("retries", Integer.valueOf(10));
        PRODUCER_DEFAULT_OVERRIDES = Collections.unmodifiableMap(tempConsumerDefaultOverrides);
        tempConsumerDefaultOverrides = new HashMap();
        tempConsumerDefaultOverrides.put("max.poll.records", "1000");
        tempConsumerDefaultOverrides.put("auto.offset.reset", "earliest");
        tempConsumerDefaultOverrides.put("enable.auto.commit", "false");
        tempConsumerDefaultOverrides.put("internal.leave.group.on.close", Boolean.valueOf(false));
        tempConsumerDefaultOverrides.put("max.poll.interval.ms", Integer.toString(2147483647));
        CONSUMER_DEFAULT_OVERRIDES = Collections.unmodifiableMap(tempConsumerDefaultOverrides);
    }

    public static class InternalConfig {
        public static final String STREAM_THREAD_INSTANCE = "__stream.thread.instance__";

        public InternalConfig() {
        }
    }
}
