package KELOS.Processors;

import KELOS.Cluster;
import KELOS.GaussianKernel;
import org.apache.commons.math3.util.Pair;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.Arrays;

import static KELOS.Main.WINDOW_TIME;

public class DensityEstimationProcessorSupplier implements ProcessorSupplier<Integer, Cluster> {
    /*
        Estimates the density for each Cluster.
     */
    private String storeName;

    public DensityEstimationProcessorSupplier(String storeName){
        this.storeName = storeName;
    }

    private class DensityEstimationProcessor implements Processor<Integer, Cluster> {
        private ProcessorContext context;
        private KeyValueStore<Integer, Cluster> clusters;
        String storeName;
        private long benchmarkTime = 0;
        private int benchmarks = 0;

        DensityEstimationProcessor(String storeName){
            this.storeName = storeName;
        }

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            this.clusters = (KeyValueStore<Integer, Cluster>) context.getStateStore(this.storeName);

            this.context.schedule(WINDOW_TIME, PunctuationType.STREAM_TIME, timestamp -> {
                long start = System.currentTimeMillis();
                // System.out.println("Density estimation at: " + timestamp);
                for(KeyValueIterator<Integer, Cluster> it = this.clusters.all(); it.hasNext();) {
                    KeyValue<Integer, Cluster> kv = it.next();
                    Integer key = kv.key;
                    Cluster cluster = kv.value;

                    ArrayList<Cluster> kNNs = new ArrayList<>();

                    for(int i : cluster.knnIds) {
                        if (this.clusters.get(i) != null) {
                            kNNs.add(this.clusters.get(i));
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
                    // System.out.println("Density for " + key);
                    this.context.forward(key, cluster);
                }

                for(KeyValueIterator<Integer, Cluster> i = this.clusters.all(); i.hasNext();) {
                    KeyValue<Integer, Cluster> cluster = i.next();

                    this.clusters.delete(cluster.key);
                }

                if(benchmarkTime == 0) {
                    benchmarkTime = System.currentTimeMillis() - start;
                } else {
                    benchmarkTime = (benchmarks * benchmarkTime + (System.currentTimeMillis() - start)) / (benchmarks + 1);
                }

                benchmarks++;

                // System.out.println("Density: " + benchmarkTime);
            });
        }

        @Override
        public void process(Integer key, Cluster cluster) {
//            System.out.println("Density process " + key);
            this.clusters.put(key, cluster);
        }

        @Override
        public void close() { }

    }

    @Override
    public Processor<Integer, Cluster> get() {
        return new DensityEstimationProcessor(this.storeName);
    }
}