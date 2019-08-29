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
    // APP CONFIG
    static final String APP_ID = "KELOS";
    static final String SERVER_CONFIGS = "localhost:9092";

    // TOPIC NAMES
    public static final String CLUSTER_ASSIGNMENT_TOPIC = "cluster-assignments";
    public static final String CLUSTER_TOPIC = "clusters";
    public static final String DENSITIES_TOPIC = "densities";
    public static final String CANDIDATES_TOPIC = "candidates";
    public static final String OUTLIERS_TOPIC = "outliers";

    // ALGORITHM CONFIGURATION
    public static final int AGGREGATION_WINDOWS = 3;
    public static final Duration WINDOW_TIME = Duration.ofSeconds(10);
    public static final double CLUSTERING_DISTANCE_THRESHOLD = 0.25;
    // K-NN neighbors
    public static final int K = 20;
    // Top-N outlier number
    public static final int N = 100;

    public static void main(final String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER_CONFIGS);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, ArrayListSerde.class.getName());

        // ======== TOPOLOGY ========

        final Topology builder = new Topology();

        builder.addSource("DataSource", InputProducer.TOPIC);

        // Global cluster store
        builder.addGlobalStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("Clusters"),
                        Serdes.Integer(),
                        new ClusterSerde()
                ).withLoggingDisabled(),
                "GlobalClustersSource",
                new IntegerDeserializer(),
                new ClusterDeserializer(),
                CLUSTER_TOPIC,
                "ClusterRegistration",
                new ClusterRegistrationProcessorSupplier());

        // ++++++++ DATA ABSTRACTOR ++++++++

        // Clustering Processor

        builder.addProcessor("ClusteringProcessor", new ClusteringProcessorSupplier(), "DataSource");

        builder.addSink("ClusterAssignmentSink", CLUSTER_ASSIGNMENT_TOPIC, new IntegerSerializer(), new TripleSerializer(), "ClusteringProcessor");

        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("TempClusters"),
                        Serdes.Integer(),
                        new ClusterSerde()),
                "ClusteringProcessor");

        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("ClusteringBuffer"),
                        Serdes.Integer(),
                        new ArrayListSerde()),
                "ClusteringProcessor");

        // Aggregation Processor

        builder.addProcessor("AggregationProcessor", new AggregationProcessorSupplier(), "ClusteringProcessor");

        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("ClusterStates"),
                        Serdes.Integer(),
                        new ClusterStatesSerde()),
                "AggregationProcessor");

        builder.addSink("ClusterSink", CLUSTER_TOPIC, new IntegerSerializer(), new ClusterSerializer(), "AggregationProcessor");

        // ++++++++ DENSITY ESTIMATOR ++++++++

        // KNN Cluster Processor

        builder.addProcessor("KNearestClusterProcessor", new KNearestClusterProcessorSupplier(), "AggregationProcessor");

        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("ClusterBuffer"),
                        Serdes.Integer(),
                        new ClusterSerde()),
                "KNearestClusterProcessor");

        // Density Estimation Processor

        builder.addProcessor("DensityEstimationProcessor", new DensityEstimationProcessorSupplier(), "KNearestClusterProcessor");
        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("ClusterDensityBuffer"),
                        Serdes.Integer(),
                        new ClusterSerde()),
                "DensityEstimationProcessor");

        builder.addSink("ClusterDensitySink", DENSITIES_TOPIC, new IntegerSerializer(), new ClusterSerializer(), "DensityEstimationProcessor");

        // Pruning Processor

        builder.addProcessor("PruningProcessor", new PruningProcessorSupplier(), "DensityEstimationProcessor");

        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("ClustersWithDensities"),
                        Serdes.Integer(),
                        new ClusterSerde()),
                "PruningProcessor");

        // Filter Processor

        builder.addProcessor("FilterProcessor", new FilterProcessorSupplier(), "PruningProcessor");

        // Window store that keeps all points of the current window
        Duration retention =  Duration.ofSeconds(AGGREGATION_WINDOWS * WINDOW_TIME.getSeconds());
        builder.addStateStore(
                Stores.windowStoreBuilder(
                        Stores.persistentWindowStore("ClusterAssignments", retention, retention, false),
                        Serdes.Integer(),
                        new TripleSerde()),
                "ClusteringProcessor", "FilterProcessor");

        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("ClustersWithCandidates"),
                        Serdes.Integer(),
                        new PairSerde()),
                "FilterProcessor");

        builder.addSink("OutlierCandidatesSink", CANDIDATES_TOPIC, new IntegerSerializer(), new PairSerializer(), "FilterProcessor");

        // ++++++++ OUTLIER DETECTOR ++++++++

        // K-Nearest Points Processor

        builder.addProcessor("KNearestPointsProcessor", new KNearestPointsProcessorSupplier(), "FilterProcessor");

        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("PointBuffer"),
                        Serdes.Integer(),
                        new ClusterSerde()),
                "KNearestPointsProcessor");

        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("CandidatePoints"),
                        Serdes.Integer(),
                        new ClusterSerde()),
                "KNearestPointsProcessor");

        // Point Density Estimation Processor

        builder.addProcessor("PointDensityEstimatorProcessor", new PointDensityEstimationProcessorSupplier(), "KNearestPointsProcessor");

        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("PointDensityBuffer"),
                        Serdes.Integer(),
                        new PairSerde()),
                "PointDensityEstimatorProcessor");

        // Point Pruning Processor

        builder.addProcessor("PointPruningProcessor", new PointPruningProcessorSupplier(), "PointDensityEstimatorProcessor");

        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("PointsWithDensities"),
                        Serdes.Integer(),
                        new PairSerde()),
                "PointPruningProcessor");

        builder.addSink("Outliers", OUTLIERS_TOPIC, new IntegerSerializer(), new ClusterSerializer(), "PointPruningProcessor");

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
