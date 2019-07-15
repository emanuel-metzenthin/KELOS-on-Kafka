package KELOS;

import KELOS.Processors.*;
import KELOS.Serdes.*;
import org.apache.kafka.common.serialization.DoubleSerializer;
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
    public static final String CANDIDATES_TOPIC = "candidates";
    public static final String PRUNED_CLUSTERS_TOPIC = "pruned_clusters";
    public static final String OUTLIERS_TOPIC = "outliers";
    public static final int AGGREGATION_WINDOWS = 1;
    public static final double DISTANCE_THRESHOLD = 0.095;
    public static final Duration WINDOW_TIME = Duration.ofSeconds(10);
    public static final int K = 40;
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
        builder.addSink("ClusterAssignmentSink", CLUSTER_ASSIGNMENT_TOPIC, new IntegerSerializer(), new TripleSerializer(), "ClusteringProcessor");
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
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("ClusterBuffer"),
                        Serdes.Integer(),
                        new ClusterSerde()),
                "KNNProcessor");

        builder.addProcessor("DensityEstimator", new DensityEstimationProcessorSupplier("ClusterDensityBuffer"), "KNNProcessor");
        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("ClusterDensityBuffer"),
                        Serdes.Integer(),
                        new ClusterSerde()),
                "DensityEstimator");
        builder.addSink("ClusterDensitySink", DENSITIES_TOPIC, new IntegerSerializer(), new ClusterSerializer(), "DensityEstimator");

        builder.addProcessor("PruningProcessor", new PruningProcessorSupplier("ClustersWithDensities", "TopNClusters"), "DensityEstimator");
        builder.addProcessor("FilterProcessor", new FilterProcessorSupplier(), "PruningProcessor");

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
                        new TripleSerde()),
                "ClusteringProcessor", "FilterProcessor");
        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("TopNClusters"),
                        Serdes.Integer(),
                        new ClusterSerde()),
                "FilterProcessor");

        builder.addSink("OutlierCandidatesSink", CANDIDATES_TOPIC, new IntegerSerializer(), new IntBoolPairSerializer(), "FilterProcessor");

        builder.addProcessor("KNNPointsProcessor", new KNearestPointsProcessorSupplier(), "FilterProcessor");

        builder.addProcessor("PointDensityEstimatorProcessor", new PointDensityEstimationProcessorSupplier("PointDensityBuffer"), "KNNPointsProcessor");

        builder.addProcessor("PointPruningProcessor", new PointPruningProcessorSupplier(), "PointDensityEstimatorProcessor");

        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("PointBuffer"),
                        Serdes.Integer(),
                        new ClusterSerde()),
                "KNNPointsProcessor");

        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("CandidatePoints"),
                        Serdes.Integer(),
                        new ClusterSerde()),
                "KNNPointsProcessor");

        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("PointDensityCandidates"),
                        Serdes.Integer(),
                        new ClusterSerde()),
                "PointDensityEstimatorProcessor");

        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("PointDensityBuffer"),
                        Serdes.Integer(),
                        new PairSerde()),
                "PointDensityEstimatorProcessor");

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
