package KELOS;

import KELOS.Processors.AggregationProcessorSupplier;
import KELOS.Processors.ClusterRegistrationProcessorSupplier;
import KELOS.Processors.ClusteringProcessorSupplier;
import KELOS.Serdes.*;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public final class ClusterProcessorService extends Service{

    static String APP_ID = "cluster-service";
    public static final int AGGREGATION_WINDOWS = 3;
    public static final double DISTANCE_THRESHOLD = 2;
    public static final Duration WINDOW_TIME = Duration.ofSeconds(1);
    static String TOPIC = "clusters";
    static String SERVER_CONFIGS = "localhost:9092";


    public static void main(final String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, ClusterProcessorService.APP_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ClusterProcessorService.SERVER_CONFIGS);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, ArrayListSerde.class.getName());

        final Topology builder = new Topology();

        builder.addSource("Source", InputProducer.TOPIC);

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
                ClusterProcessorService.TOPIC,
                "ClusterRegistration",
                new ClusterRegistrationProcessorSupplier());

        builder.addProcessor("ClusteringProcessor", new ClusteringProcessorSupplier(), "Source");
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

        builder.addSink("Sink", ClusterProcessorService.TOPIC, new IntegerSerializer(), new ClusterSerializer(), "AggregationProcessor");

        shutdown(builder, props);
    }
}