package KELOS.Processors;

import KELOS.Cluster;
import KELOS.GaussianKernel;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
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
                this.clusters = (KeyValueStore<Integer, Cluster>) context.getStateStore("Clusters");
            }

            @Override
            public void process(Integer key, Cluster cluster) {
                ArrayList<Cluster> kNNs = new ArrayList<>();

                for(int i : cluster.knnIds) {
                    if (this.clusters.get(i) != null){
                        kNNs.add(this.clusters.get(i));
                    }
                }

                if (kNNs.size() <= 1) {
                    return;
                }

                int k = kNNs.size();
                int d = kNNs.get(0).centroid.length;

                ArrayList<Double> clusterWeights = new ArrayList<>();

                int totalSize = kNNs.stream().mapToInt(cl -> cl.size).sum();

                // TODO fix empty clusters
                if (totalSize == 0) {
                    return;
                }

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

                double density = 0;

                for(int i = 0; i < k; i++) {
                    double productKernel = 1;

                    for(int j = 0; j < d; j++) {
                        double difference = Math.abs(cluster.centroid[j] - kNNs.get(i).centroid[j]);
                        productKernel *= new GaussianKernel(dimensionBandwidths.get(j)).computeDensity(difference);
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