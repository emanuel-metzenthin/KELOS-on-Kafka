package KELOS.Processors;

import KELOS.Cluster;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;


import java.util.ArrayList;
import java.util.Comparator;

import static KELOS.Main.*;


public class PruningProcessorSupplier implements ProcessorSupplier<Integer, Cluster> {

    /*
        Calculates the KLOME_low and KLOME_high for each cluster, then prunes clusters that are guaranteed to not
        contain any outliers.
     */

    @Override
    public Processor<Integer, Cluster> get() {
        return new Processor<Integer, Cluster>() {
            private ProcessorContext context;
            private KeyValueStore<Integer, Cluster> clusterWithDensities;
            private long benchmarkTime = 0;
            private int benchmarks = 0;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                this.clusterWithDensities = (KeyValueStore<Integer, Cluster>) context.getStateStore("ClustersWithDensities");
            }

            @Override
            public void process(Integer key, Cluster value) {

                if (Cluster.isEndOfWindowToken(value)) {
                    long start = System.currentTimeMillis();

                    ArrayList<Triple<Integer, Double, Double>> clusters_with_klome = new ArrayList<>();

                    for (KeyValueIterator<Integer, Cluster> i = this.clusterWithDensities.all(); i.hasNext(); ) {
                        KeyValue<Integer, Cluster> cluster = i.next();

                        double knnMean = 0;
                        double knnVariance = 0;

                        ArrayList<Integer> existingKnnIds = new ArrayList<>();

                        for (int id : cluster.value.knnIds) {
                            if (this.clusterWithDensities.get(id) != null) {
                                existingKnnIds.add(id);
                                knnMean += this.clusterWithDensities.get(id).density;
                            }
                        }

                        knnMean /= existingKnnIds.size();

                        for (int id : existingKnnIds) {
                            knnVariance += Math.pow((this.clusterWithDensities.get(id).density - knnMean), 2);
                        }

                        double knnStddev = Math.sqrt(knnVariance);

                        double klomeLow = (cluster.value.minDensityBound - knnMean) / knnStddev;
                        double klomeHigh = (cluster.value.maxDensityBound - knnMean) / knnStddev;
                        Triple<Integer, Double, Double> triple = Triple.of(cluster.key, klomeLow, klomeHigh);

                        clusters_with_klome.add(triple);
                    }

                    int[] before_counts = new int[clusters_with_klome.size()];

                    for (int i = 0; i < clusters_with_klome.size(); i++) {
                        Triple<Integer, Double, Double> t1 = clusters_with_klome.get(i);
                        int size = this.clusterWithDensities.get(t1.getLeft()).size;

                        for (int j = 0; j < clusters_with_klome.size(); j++) {
                            Triple<Integer, Double, Double> t2 = clusters_with_klome.get(j);
                            if (t1.getRight() < t2.getMiddle()) {
                                before_counts[j] += size;
                            }
                        }
                    }

                    for (int i = 0; i < before_counts.length; i++) {
                        if (before_counts[i] < N) {
                            int cluster = clusters_with_klome.get(i).getLeft();

                            this.context.forward(cluster, this.clusterWithDensities.get(cluster));
                        }
                    }

                    this.context.forward(key, value);

                    for (KeyValueIterator<Integer, Cluster> i = this.clusterWithDensities.all(); i.hasNext(); ) {
                        KeyValue<Integer, Cluster> cluster = i.next();

                        this.clusterWithDensities.delete(cluster.key);
                    }

                    context.commit();

                    if (benchmarkTime == 0) {
                        benchmarkTime = System.currentTimeMillis() - start;
                    } else {
                        benchmarkTime = (benchmarks * benchmarkTime + (System.currentTimeMillis() - start)) / (benchmarks + 1);
                    }

                    benchmarks++;

                    System.out.println("Pruning Cluster: " + benchmarkTime);
                } else {
                    this.clusterWithDensities.put(key, value);
                }
            }

            @Override
            public void close() { }
        };
    }
}
