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

    static final int K = 5;

    static class KNearestClusterProcessorSupplier implements ProcessorSupplier<Integer, Cluster> {
        /*
            Finds the K nearest neighbors for each input Cluster.
         */
        @Override
        public Processor<Integer, Cluster> get() {
            return new Processor<Integer, Cluster>() {
                private ProcessorContext context;
                private WindowStore<Integer, Cluster> clusters;

                @Override
                public void init(ProcessorContext context) {
                    this.context = context;
                    this.clusters = (WindowStore<Integer, Cluster>) context.getStateStore("ClusterBuffer");

                    this.context.schedule(ClusterProcessorService.WINDOW_TIME, PunctuationType.STREAM_TIME, timestamp -> {
                        for(KeyValueIterator<Windowed<Integer>, Cluster> i = this.clusters.fetchAll(timestamp - ClusterProcessorService.WINDOW_TIME.toMillis() , timestamp); i.hasNext();) {
                            KeyValue<Windowed<Integer>, Cluster> cluster = i.next();

                            cluster.value.calculateKNearestNeighbors(this.clusters.fetchAll(timestamp - ClusterProcessorService.WINDOW_TIME.toMillis() , timestamp));

                            context.forward(cluster.key, cluster.value);
                        }

                        context.commit();
                    });
                }

                @Override
                public void process(Integer key, Cluster value) {
                    this.clusters.put(key, value);
                }

                @Override
                public void close() { }
            };
        }
    }

    static class DensityEstimationProcessorSupplier implements ProcessorSupplier<Integer, Cluster> {
        /*
            Estimates the density for each Cluster.
         */
        @Override
        public Processor<Integer, Cluster> get() {
            return new Processor<Integer, Cluster>() {
                private ProcessorContext context;
                private KeyValueStore<Integer, Cluster> clusters;

                @Override
                public void init(ProcessorContext context) {
                    this.context = context;
                    this.clusters = (KeyValueStore<Integer, Cluster>) context.getStateStore("Clusters");
                }

                @Override
                public void process(Integer key, Cluster cluster) {
                    ArrayList<Cluster> kNNs = new ArrayList<>();

                    for(int i : cluster.knnIds) {
                        kNNs.add(this.clusters.get(i));
                    }

                    int k = kNNs.size();
                    int d = kNNs.get(0).centroid.length;

                    ArrayList<Double> clusterWeights = new ArrayList<>();

                    for(Cluster c : kNNs) {
                        clusterWeights.add((double) (c.size / kNNs.stream().mapToInt(cl -> cl.size).sum()));
                    }

                    ArrayList<Double> dimensionMeans = new ArrayList<>();

                    for(int i = 0; i < d; i++) {
                        double mean = 0;

                        for(int m = 0; m < k; m++) {
                            mean += kNNs.get(m).centroid[i] * clusterWeights.get(m);
                        }

                        mean /= k;

                        dimensionMeans.add(mean);
                    }

                    ArrayList<Double> dimensionStdDevs = new ArrayList<>();

                    for(int i = 0; i < d; i++) {
                        double stdDev = 0;

                        for(int m = 0; m < k; m++) {
                            double diffToMean = kNNs.get(m).centroid[i] - dimensionMeans.get(m);
                            stdDev += Math.pow(diffToMean, 2) * clusterWeights.get(m);
                        }

                        stdDev = Math.sqrt(stdDev);

                        dimensionStdDevs.add(stdDev);
                    }

                    ArrayList<Double> dimensionBandwidths = new ArrayList<>();

                    for(int i = 0; i < d; i++) {
                        double bandwidth = 1.06 * dimensionStdDevs.get(i) * Math.pow(k, -1 / (d + 1));
                        dimensionBandwidths.add(bandwidth);
                    }

                    double density = 0;

                    for(int i = 0; i < k; i++) {
                        double productKernel = 1;

                        for(int j = 0; j < d; j++) {
                            // TODO: This should be the difference between the centroid of this cluster and cluster i from the KNN
                            double difference = Math.abs(cluster.centroid[j]);
                            productKernel *= new GaussianKernel(dimensionBandwidths.get(i)).computeDensity(difference);
                        }

                        density += productKernel * clusterWeights.get(i);
                    }

                    this.context.forward(key, density);
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

        Duration retention =  Duration.ofSeconds(ClusterProcessorService.AGGREGATION_WINDOWS * ClusterProcessorService.WINDOW_TIME.getSeconds());
        builder.addStateStore(
                Stores.windowStoreBuilder(
                        Stores.persistentWindowStore("KNearestClusters", retention, ClusterProcessorService.WINDOW_TIME, false),
                        Serdes.Integer(),
                        new ClusterSerde()),
                "ClusterBuffer");

        builder.addProcessor("DensityEstimator", new DensityEstimationService.DensityEstimationProcessorSupplier(), "KNNProcessor");

        builder.addSink("Sink", DensityEstimationService.TOPIC, new IntegerSerializer(), new DoubleSerializer(), "DensityEstimator");

        shutdown(builder, props);
    }
}
