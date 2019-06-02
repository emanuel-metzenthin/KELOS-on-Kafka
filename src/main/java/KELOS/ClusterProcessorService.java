package KELOS;

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

public final class ClusterProcessorService {

    static String APP_ID = "cluster-service";
    static final int AGGREGATION_WINDOWS = 3;
    static final double DISTANCE_THRESHOLD = 2;
    static final Duration WINDOW_TIME = Duration.ofSeconds(1);
    static String TOPIC = "clusters";
    static String SERVER_CONFIGS = "localhost:9092";

    static class ClusterRegistrationProcessorSupplier implements ProcessorSupplier<Integer, Cluster> {

        @Override
        public Processor<Integer, Cluster> get() {
            return new Processor<Integer, Cluster>() {
                private KeyValueStore<Integer, Cluster> state;

                @Override
                public void init(ProcessorContext context) {
                    this.state = (KeyValueStore<Integer, Cluster>) context.getStateStore("Clusters");
                }

                @Override
                public void process(Integer key, Cluster value) {
                    this.state.put(key, value);
                }

                @Override
                public void close() { }
            };
        }
    }

    static class ClusteringProcessorSupplier implements ProcessorSupplier<String, ArrayList<Double>> {

        /*
            Clusters data points in sub-windows and emits cluster meta-data for the ClusterRegistrationProcessor
            to aggregate them into the global store.
         */
        @Override
        public Processor<String, ArrayList<Double>> get() {
            return new Processor<String, ArrayList<Double>>() {
                private ProcessorContext context;
                private KeyValueStore<Integer, Cluster> tempClusters;

                @Override
                public void init(ProcessorContext context) {
                    this.context = context;
                    this.tempClusters = (KeyValueStore<Integer, Cluster>) context.getStateStore("TempClusters");
                    KeyValueStore<Integer, Cluster> clusters = (KeyValueStore<Integer, Cluster>) context.getStateStore("Clusters");

                    // Emit cluster meta data after sub-window has been processed
                    this.context.schedule(ClusterProcessorService.WINDOW_TIME, PunctuationType.STREAM_TIME, timestamp -> {
                        for(KeyValueIterator<Integer, Cluster> i = this.tempClusters.all(); i.hasNext();) {
                            KeyValue<Integer, Cluster> cluster = i.next();
                            context.forward(cluster.key, cluster.value);
                        }

                        context.commit();

                        // Clear all meta data in cluster store, but keep centroids for distance computation
                        for(KeyValueIterator<Integer, Cluster> i = clusters.all(); i.hasNext();) {
                            KeyValue<Integer, Cluster> cluster = i.next();
                            Cluster emptyCluster = new Cluster(cluster.value.centroid.length);
                            System.out.println("New Cluster of size: " + cluster.value.centroid.length);
                            emptyCluster.centroid = cluster.value.centroid;

                            this.tempClusters.put(cluster.key, emptyCluster);
                        }
                    });
                }

                /*
                    Clusters data points by computing distances to cluster centroids
                    and adding the points to the nearest cluster or by creating
                    new clusters for distances above the threshold.
                */
                @Override
                public void process(String key, ArrayList<Double> value) {

                    double minDist = Double.MAX_VALUE;
                    Cluster cluster = null;
                    int clusterIdx = 0;
                    int numCluster = 0; // Highest cluster index, needed to create new clusters

                    for(KeyValueIterator<Integer, Cluster> i = this.tempClusters.all(); i.hasNext();) {
                        KeyValue<Integer, Cluster> c = i.next();

                        double dist = c.value.distance(value);

                        if (dist < minDist) {
                            minDist = dist;
                            cluster = c.value;
                            clusterIdx = c.key;
                        }

                        numCluster++;
                    }

                    if (minDist < ClusterProcessorService.DISTANCE_THRESHOLD) {
                        cluster.addRecord(value);
                        this.tempClusters.put(clusterIdx, cluster);
                    } else {
                        System.out.println("New Cluster of size for existing point: " + value.size());
                        this.tempClusters.put(numCluster + 1, new Cluster(value));
                    }
                }

                @Override
                public void close() { }
            };
        }
    }

    static class AggregationProcessorSupplier implements ProcessorSupplier<Integer, Cluster> {

        /*
            Aggregates the sub-windows and emits the aggregated clusters.
            Stores lists of the states of individual clusters in a local store,
            merges new arriving clusters with their old states.
         */
        @Override
        public Processor<Integer, Cluster> get() {
            return new Processor<Integer, Cluster>() {
                private ProcessorContext context;
                private KeyValueStore<Integer, ArrayList<Cluster>> clusterStates;

                @Override
                public void init(ProcessorContext context) {
                    this.context = context;
                    this.clusterStates = (KeyValueStore<Integer, ArrayList<Cluster>>) context.getStateStore("ClusterStates");
                }

                @Override
                public void process(Integer key, Cluster value) {

                    ArrayList<Cluster> oldList = this.clusterStates.get(key);

                    if (oldList == null || oldList.get(0) == null) {
                        ArrayList<Cluster> newList = new ArrayList<>();
                        newList.add(value);

                        this.context.forward(key, value);
                        this.clusterStates.put(key, newList);
                    } else {
                        ArrayList<Cluster> newList = oldList;

                        if (oldList.size() > ClusterProcessorService.AGGREGATION_WINDOWS){
                            newList.remove(0);
                        }

                        Cluster aggregate = value;

                        for (Cluster c : newList){
                            aggregate.merge(c);
                        }

                        newList.add(value);

                        this.context.forward(key, aggregate);
                        this.clusterStates.put(key, newList);
                    }
                }

                @Override
                public void close() { }
            };
        }
    }

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



        // ==== SHUTDOWN ====

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