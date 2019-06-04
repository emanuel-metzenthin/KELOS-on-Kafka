package KELOS;

import KELOS.Serdes.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class DensityEstimationService {

    static String APP_ID = "density-estimation-service";
    static String TOPIC = "clusters_with_density";
    static String SERVER_CONFIGS = "localhost:9092";

    static class KNearestClusterProcessorSupplier implements ProcessorSupplier<Integer, Cluster> {

        /*
            Finds the K nearest neighbors for each input Cluster.
         */
        @Override
        public Processor<Integer, Cluster> get() {
            return new Processor<Integer, Cluster>() {
                private ProcessorContext context;
                private KeyValueStore<Integer, Cluster> tempClusters;

                @Override
                public void init(ProcessorContext context) {
                    this.context = context;
                    this.tempClusters = (KeyValueStore<Integer, Cluster>) context.getStateStore("KNearestClusters");
                }

                /*

                */
                @Override
                public void process(Integer key, Cluster value) {

                }

                @Override
                public void close() { }
            };
        }
    }

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

        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("KNearestClusters"),
                        Serdes.Integer(),
                        new ClusterSerde()),
                "KNNProcessor");

        builder.addSink("Sink", DensityEstimationService.TOPIC, new IntegerSerializer(), new ClusterSerializer(), "KNNProcessor");


        // ==== SHUTDOWN ====

        final KafkaStreams streams = new KafkaStreams(builder, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-density-estimation-shutdown-hook") {
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
