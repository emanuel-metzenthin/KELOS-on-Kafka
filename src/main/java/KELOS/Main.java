package KELOS;

import KELOS.Processors.DensityEstimationProcessorSupplier;
import KELOS.Processors.KNearestClusterProcessorSupplier;
import KELOS.Serdes.ClusterDeserializer;
import KELOS.Serdes.ClusterSerde;
import KELOS.Serdes.ClusterSerializer;
import KELOS.Serdes.ClusterStatesSerde;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.Properties;

import static KELOS.Service.shutdown;

public class Main {
    public static void main(final String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, DensityEstimationService.APP_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, DensityEstimationService.SERVER_CONFIGS);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, ClusterSerde.class.getName());

        final Topology builder = new Topology();

        builder.addSource("Source", InputProducer.TOPIC);

        builder.addProcessor("ClusteringProcessor", new ClusterProcessorService.ClusteringProcessorSupplier(), "Source");
        builder.addProcessor("AggregationProcessor", new ClusterProcessorService.AggregationProcessorSupplier(), "ClusteringProcessor");

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

        builder.addSink("Sink", ClusterProcessorService.TOPIC, new IntegerSerializer(), new ClusterSerializer(), "AggregationProcessor");

        builder.addSource("Source", ClusterProcessorService.TOPIC);

        builder.addProcessor("KNNProcessor", new KNearestClusterProcessorSupplier(), "Source");

        Duration retention =  Duration.ofSeconds(ClusterProcessorService.AGGREGATION_WINDOWS * ClusterProcessorService.WINDOW_TIME.getSeconds());
        builder.addStateStore(
                Stores.windowStoreBuilder(
                        Stores.persistentWindowStore("ClusterBuffer", retention, ClusterProcessorService.WINDOW_TIME, false),
                        Serdes.Integer(),
                        new ClusterSerde()),
                "KNNProcessor");

        builder.addProcessor("DensityEstimator", new DensityEstimationProcessorSupplier(), "KNNProcessor");

        builder.addSink("Sink", DensityEstimationService.TOPIC, new IntegerSerializer(), new DoubleSerializer(), "DensityEstimator");

        shutdown(builder, props);
    }
}
