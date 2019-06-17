package KELOS;

import KELOS.Serdes.*;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.kafka.common.serialization.DoubleSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.WindowStore;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;

public class DensityEstimationService extends Service {

    static String APP_ID = "density-estimation-service";
    static String TOPIC = "clusters-with-density";
    static String SERVER_CONFIGS = "localhost:9092";

    public static final int K = 5;

    public static void main(final String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, DensityEstimationService.APP_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, DensityEstimationService.SERVER_CONFIGS);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, ClusterSerde.class.getName());

        final Topology builder = new Topology();
        builder.addSource("Source", ClusterProcessorService.TOPIC);

        builder.addProcessor("KNNProcessor", new DensityEstimationService.KNearestClusterProcessorSupplier(), "Source");

        Duration retention =  Duration.ofSeconds(ClusterProcessorService.AGGREGATION_WINDOWS * ClusterProcessorService.WINDOW_TIME.getSeconds());
        builder.addStateStore(
                Stores.windowStoreBuilder(
                        Stores.persistentWindowStore("ClusterBuffer", retention, ClusterProcessorService.WINDOW_TIME, false),
                        Serdes.Integer(),
                        new ClusterSerde()),
                "KNNProcessor");

        builder.addProcessor("DensityEstimator", new DensityEstimationService.DensityEstimationProcessorSupplier(), "KNNProcessor");

        builder.addSink("Sink", DensityEstimationService.TOPIC, new IntegerSerializer(), new DoubleSerializer(), "DensityEstimator");

        shutdown(builder, props);
    }
}
