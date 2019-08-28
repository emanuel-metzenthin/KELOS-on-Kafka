package KELOS.Processors;

import KELOS.Cluster;
import KELOS.GaussianKernel;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;

public class DensityEstimationProcessorSupplier implements ProcessorSupplier<Integer, Cluster> {

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
                this.clusters = (KeyValueStore<Integer, Cluster>) context.getStateStore("ClusterDensityBuffer");
            }

            @Override
            public void process(Integer key, Cluster cluster) {
                // At end of window
                if (Cluster.isEndOfWindowToken(cluster)){
                    for(KeyValueIterator<Integer, Cluster> it = this.clusters.all(); it.hasNext();) {
                        KeyValue<Integer, Cluster> kv = it.next();
                        Integer storeKey = kv.key;
                        Cluster storeCluster = kv.value;

                        // Store nearest neighbor clusters
                        ArrayList<Cluster> kNNs = new ArrayList<>();

                        for(int i : storeCluster.knnIds) {
                            if (this.clusters.get(i) != null) {
                                kNNs.add(this.clusters.get(i));
                            }
                        }

                        if (kNNs.size() <= 1) {
                            continue;
                        }

                        int k = kNNs.size();
                        int d = kNNs.get(0).centroid.length;

                        // Compute the weight for each neighbor cluster

                        ArrayList<Double> clusterWeights = new ArrayList<>();

                        int totalSize = kNNs.stream().mapToInt(cl -> cl.size).sum();

                        for(Cluster c : kNNs) {
                            clusterWeights.add((double) c.size / totalSize);
                        }

                        // Compute the means per dimension
                        ArrayList<Double> dimensionMeans = new ArrayList<>();

                        for(int i = 0; i < d; i++) {
                            double mean = 0;

                            for(int m = 0; m < k; m++) {
                                mean += kNNs.get(m).centroid[i] * clusterWeights.get(m);
                            }

                            mean /= k;

                            dimensionMeans.add(mean);
                        }

                        // Compute the standard deviations per dimension
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

                        // Compute the bandwidths per dimension
                        ArrayList<Double> dimensionBandwidths = new ArrayList<>();

                        for(int i = 0; i < d; i++) {
                            double bandwidth = 1.06 * dimensionStdDevs.get(i) * Math.pow(k, -1.0 / (d + 1));
                            dimensionBandwidths.add(bandwidth);
                        }

                        // === Compute density ===

                        storeCluster.density = 0;
                        storeCluster.minDensityBound = 0;
                        storeCluster.maxDensityBound = 0;

                        for(int i = 0; i < k; i++) {
                            double productKernel = 1;
                            double minProductKernel = 1;
                            double maxProductKernel = 1;

                            for(int j = 0; j < d; j++) {
                                double difference = Math.abs(storeCluster.centroid[j] - kNNs.get(i).centroid[j]);
                                double distToMin = storeCluster.centroid[j] - storeCluster.minimums[j];
                                double distToMax = storeCluster.maximums[j] - storeCluster.centroid[j];
                                double radius = Math.max(distToMin, distToMax);
                                productKernel *= new GaussianKernel(dimensionBandwidths.get(j)).computeDensity(difference);
                                minProductKernel *= new GaussianKernel(dimensionBandwidths.get(j)).computeDensity(difference + radius);
                                maxProductKernel *= new GaussianKernel(dimensionBandwidths.get(j)).computeDensity(Math.max(difference - radius, 0));
                            }

                            storeCluster.density += productKernel * clusterWeights.get(i);
                            storeCluster.minDensityBound += minProductKernel * clusterWeights.get(i);
                            storeCluster.maxDensityBound += maxProductKernel * clusterWeights.get(i);
                        }

                        this.context.forward(storeKey, storeCluster);
                    }

                    // Forward EndOfWindowToken
                    this.context.forward(key, cluster);

                    for(KeyValueIterator<Integer, Cluster> i = this.clusters.all(); i.hasNext();) {
                        KeyValue<Integer, Cluster> storeCluster = i.next();

                        this.clusters.delete(storeCluster.key);
                    }
                }

                // If not end of window
                this.clusters.put(key, cluster);
            }

            @Override
            public void close() { }

        };
    }
}