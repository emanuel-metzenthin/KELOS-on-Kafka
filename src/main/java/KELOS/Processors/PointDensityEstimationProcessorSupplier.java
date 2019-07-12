package KELOS.Processors;

import KELOS.Cluster;
import KELOS.GaussianKernel;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;

import static KELOS.Main.WINDOW_TIME;

public class PointDensityEstimationProcessorSupplier implements ProcessorSupplier<Integer, Pair<Cluster, Integer>> {
    /*
        Estimates the density for each Cluster.
     */
    private String storeName;

    public PointDensityEstimationProcessorSupplier(String storeName){
        this.storeName = storeName;
    }

    private class DensityEstimationProcessor implements Processor<Integer, Pair<Cluster, Integer>> {
        private ProcessorContext context;
        private KeyValueStore<Integer, Pair<Cluster, Integer>> windowPoints;
        String storeName;

        DensityEstimationProcessor(String storeName){
            this.storeName = storeName;
        }

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            this.windowPoints = (KeyValueStore<Integer, Pair<Cluster, Integer>>) context.getStateStore("PointDensityBuffer");

            this.context.schedule(WINDOW_TIME, PunctuationType.STREAM_TIME, timestamp -> {
                System.out.println("Point density estimation at: " + timestamp);
                for(KeyValueIterator<Integer, Pair<Cluster, Integer>> it = this.windowPoints.all(); it.hasNext();) {
                    KeyValue<Integer, Pair<Cluster, Integer>> kv = it.next();
                    Integer key = kv.key;

                    // Don't compute density for neighbors of neighbors
                    if (kv.value.getRight() == 2){
                        continue;
                    }

                    Cluster cluster = kv.value.getLeft();

                    ArrayList<Cluster> kNNs = new ArrayList<>();

                    for(int i : cluster.knnIds) {
                        if (this.windowPoints.get(i) != null) {
                            kNNs.add(this.windowPoints.get(i).getLeft());
                        }
                    }

                    if (kNNs.size() <= 1) {
                        continue;
                    }

                    int k = kNNs.size();
                    int d = kNNs.get(0).centroid.length;

                    ArrayList<Double> clusterWeights = new ArrayList<>();

                    int totalSize = kNNs.stream().mapToInt(cl -> cl.size).sum();

                    for(Cluster c : kNNs) {
                        clusterWeights.add((double) c.size / totalSize);
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
                            double diffToMean = kNNs.get(m).centroid[i] - dimensionMeans.get(i);
                            stdDev += Math.pow(diffToMean, 2) * clusterWeights.get(m);
                        }

                        stdDev = Math.sqrt(stdDev);

                        dimensionStdDevs.add(stdDev);
                    }

                    ArrayList<Double> dimensionBandwidths = new ArrayList<>();

                    for(int i = 0; i < d; i++) {
                        double bandwidth = 1.06 * dimensionStdDevs.get(i) * Math.pow(k, -1.0 / (d + 1));
                        dimensionBandwidths.add(bandwidth);
                    }

                    cluster.density = 0;
                    cluster.minDensityBound = 0;
                    cluster.maxDensityBound = 0;

                    for(int i = 0; i < k; i++) {
                        double productKernel = 1;
                        double minProductKernel = 1;
                        double maxProductKernel = 1;

                        for(int j = 0; j < d; j++) {
                            double difference = Math.abs(cluster.centroid[j] - kNNs.get(i).centroid[j]);
                            double distToMin = cluster.centroid[j] - cluster.minimums[j];
                            double distToMax = cluster.maximums[j] - cluster.centroid[j];
                            double radius = Math.max(distToMin, distToMax);
                            productKernel *= new GaussianKernel(dimensionBandwidths.get(j)).computeDensity(difference);
                            minProductKernel *= new GaussianKernel(dimensionBandwidths.get(j)).computeDensity(difference + radius);
                            maxProductKernel *= new GaussianKernel(dimensionBandwidths.get(j)).computeDensity(Math.max(difference - radius, 0));
                        }

                        cluster.density += productKernel * clusterWeights.get(i);
                        cluster.minDensityBound += minProductKernel * clusterWeights.get(i);
                        cluster.maxDensityBound += maxProductKernel * clusterWeights.get(i);
                    }
//                    if(this.storeName == "PointDensityBuffer"){
//                        System.out.println("Dens forward " + kv.key);
//                    }
                    System.out.println("Point density for " + key);
                    this.context.forward(key, Pair.of(cluster, kv.value.getRight()));
                }

                for(KeyValueIterator<Integer, Pair<Cluster, Integer>> i = this.windowPoints.all(); i.hasNext();) {
                    KeyValue<Integer, Pair<Cluster, Integer>> cluster = i.next();

                    this.windowPoints.delete(cluster.key);
                }
            });
        }

        @Override
        public void process(Integer key, Pair<Cluster, Integer> cluster) {
            System.out.println("Point density process " + key);
            this.windowPoints.put(key, cluster);
        }

        @Override
        public void close() { }

    }

    @Override
    public Processor<Integer, Pair<Cluster, Integer>> get() {
        return new DensityEstimationProcessor(this.storeName);
    }
}