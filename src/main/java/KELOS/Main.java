package KELOS;

import KELOS.Processors.*;
import KELOS.Serdes.*;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class Main {
    static final String APP_ID = "KELOS";
    static final String SERVER_CONFIGS = "localhost:9092";

    public static final String CLUSTER_ASSIGNMENT_TOPIC = "cluster-assignments";
    public static final String CLUSTER_TOPIC = "clusters";
    public static final String DENSITIES_TOPIC = "densities";
    public static final String PRUNED_CLUSTERS_TOPIC = "pruned_clusters";
    public static final int AGGREGATION_WINDOWS = 3;
    public static final double DISTANCE_THRESHOLD = 0.5;
    public static final Duration WINDOW_TIME = Duration.ofSeconds(1);
    public static final int K = 5;
    public static final int N = 5;

    public static void main(final String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_CONFIGS);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, ArrayListSerde.class.getName());

        final Topology builder = new Topology();

        builder.addSource("DataSource", InputProducer.TOPIC);

        // Add global cluster store
        builder.addGlobalStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("Clusters"),
                        Serdes.Integer(),
                        new ClusterSerde()
                ).withLoggingDisabled(),
                "GlobalSource",
                new IntegerDeserializer(),
                new ClusterDeserializer(),
                CLUSTER_TOPIC,
                "ClusterRegistration",
                new ClusterRegistrationProcessorSupplier());

        builder.addProcessor("ClusteringProcessor", new ClusteringProcessorSupplier(), "DataSource");
        builder.addSink("ClusterAssignmentSink", CLUSTER_ASSIGNMENT_TOPIC, new IntegerSerializer(), new ArrayListSerializer(), "ClusteringProcessor");
        builder.addProcessor("AggregationProcessor", new AggregationProcessorSupplier(), "ClusteringProcessor");

        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("TempClusters"),
                        Serdes.Integer(),
                        new ClusterSerde()),
                "ClusteringProcessor");

        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("ClusterStates"),
                        Serdes.Integer(),
                        new ClusterStatesSerde()),
                "AggregationProcessor");

        builder.addSink("ClusterSink", CLUSTER_TOPIC, new IntegerSerializer(), new ClusterSerializer(), "AggregationProcessor");

        builder.addProcessor("KNNProcessor", new KNearestClusterProcessorSupplier(), "AggregationProcessor");

        Duration retention =  Duration.ofSeconds(AGGREGATION_WINDOWS * WINDOW_TIME.getSeconds());
        builder.addStateStore(
                Stores.windowStoreBuilder(
                        Stores.persistentWindowStore("ClusterBuffer", retention, retention, false),
                        Serdes.Integer(),
                        new ClusterSerde()),
                "KNNProcessor");

        builder.addProcessor("DensityEstimator", new DensityEstimationProcessorSupplier(), "KNNProcessor");
        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("ClusterDensityBuffer"),
                        Serdes.Integer(),
                        new ClusterSerde()),
                "DensityEstimator");
        builder.addSink("ClusterDensitySink", DENSITIES_TOPIC, new IntegerSerializer(), new ClusterSerializer(), "DensityEstimator");

        builder.addProcessor("PruningProcessor", new PruningProcessorSupplier(), "DensityEstimator");
        builder.addProcessor("FilterProcessor", new FilterProcessorSupplier(), "ClusteringProcessor");
        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("ClustersWithDensities"),
                        Serdes.Integer(),
                        new ClusterSerde()),
                "PruningProcessor");
        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("ClusterAssignments"),
                        Serdes.Integer(),
                        new ArrayListSerde()),
                "FilterProcessor");
        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("TopNClusters"),
                        Serdes.Integer(),
                        new ClusterSerde()),
                "PruningProcessor", "FilterProcessor");

        builder.addSink("PrunedSink", PRUNED_CLUSTERS_TOPIC, new IntegerSerializer(), new ClusterSerializer(), "PruningProcessor");

        shutdown(builder, props);
    }

    private static void shutdown(Topology builder, Properties props) {
        final KafkaStreams streams = new KafkaStreams(builder, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
