import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public final class ClusterService {

    static String TOPIC = "clusters";
    static String APP_ID = "cluster-service";
    static String SERVER_CONFIGS = "localhost:9092";

    public static void main(final String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, ClusterService.APP_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,ClusterService.SERVER_CONFIGS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, ArrayListSerde.class.getName());

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, ArrayListSerde> data = builder.stream(InputProducer.TOPIC);

        final GlobalKTable<Integer, Cluster> clusters = builder.globalTable(TOPIC);

        final KGroupedStream<String, ArrayListSerde> windows = data
                .join(clusters, (dataPoint, cluster) -> cluster) // Häää?
                .groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(5)).advanceBy(Duration.ofSeconds(1)));

        // ==== SHUTDOWN ====

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("cluster-service-shutdown-hook") {
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