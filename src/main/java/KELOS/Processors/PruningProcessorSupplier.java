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

    private class KlomeComparator implements Comparator<Triple<Integer, Double, Double>> {
        @Override
        public int compare(Triple<Integer, Double, Double> val1, Triple<Integer, Double, Double> val2)
        {
            return val1.getRight().compareTo(val2.getRight());
        }
    }

    /*
        Calculates the KLOME_low and KLOME_high for each cluster
     */
    private String densityStoreName;
    private String topNStoreName;

    public PruningProcessorSupplier(String densityStoreName, String topNStoreName){
        this.densityStoreName = densityStoreName;
        this.topNStoreName = topNStoreName;
    }

    private class PruningProcessor implements Processor<Integer, Cluster> {
        private ProcessorContext context;
        private KeyValueStore<Integer, Cluster> clusterWithDensities;
        private KeyValueStore<Integer, Cluster> topNClusters;
        String densityStoreName;
        String topNStoreName;
        private long benchmarkTime = 0;
        private int benchmarks = 0;

        PruningProcessor(String densityStoreName, String topNStoreName){
            this.densityStoreName = densityStoreName;
            // this.topNStoreName = topNStoreName;
        }

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            this.clusterWithDensities = (KeyValueStore<Integer, Cluster>) context.getStateStore(this.densityStoreName);
            // this.topNClusters = (KeyValueStore<Integer, Cluster>) context.getStateStore(this.topNStoreName);
        }

        /*

         */
        @Override
        public void process(Integer key, Cluster value) {

            if (Cluster.isEndOfWindowToken(value)){
                long start = System.currentTimeMillis();
                /*MinMaxPriorityQueue<Triple<Integer, Double, Double>> queue = MinMaxPriorityQueue
                        .orderedBy(new KlomeComparator())
                        // .maximumSize(N)
                        .create();*/

                ArrayList<Triple<Integer, Double, Double>> clusters_with_klome = new ArrayList<>();

                // System.out.println("Pruning at " + timestamp);
                for(KeyValueIterator<Integer, Cluster> i = this.clusterWithDensities.all(); i.hasNext();) {
                    KeyValue<Integer, Cluster> cluster = i.next();

                    // System.out.println("Pruning cluster " + cluster.key);

                    double knnMean = 0;
                    double knnVariance = 0;

                    ArrayList<Integer> existingKnnIds = new ArrayList<>();

                    for (int id : cluster.value.knnIds){
                        if (this.clusterWithDensities.get(id) != null) {
                            existingKnnIds.add(id);
                            knnMean += this.clusterWithDensities.get(id).density;
                        }
                    }

                    knnMean /= existingKnnIds.size();

                    for (int id : existingKnnIds){
                        knnVariance += Math.pow((this.clusterWithDensities.get(id).density - knnMean), 2);
                    }

                    double knnStddev = Math.sqrt(knnVariance);

                    double klomeLow = (cluster.value.minDensityBound - knnMean) / knnStddev;
                    double klomeHigh = (cluster.value.maxDensityBound - knnMean) / knnStddev;
                    Triple<Integer, Double, Double> triple = Triple.of(cluster.key, klomeLow, klomeHigh);

                    // System.out.println("Cluster: " + cluster.key + " KLOMELow: " + klomeLow + " KLOMEHigh: " + klomeHigh);

                    clusters_with_klome.add(triple);

                    /*if (queue.size() < N){
                        queue.add(triple);
                    }
                    else if (klomeHigh < queue.peekLast().getMiddle()){
                        queue.pollLast();
                        queue.add(triple);
                    }
                    else if (klomeLow <= queue.peekLast().getRight()){
                        queue.add(triple);
                    }*/
                }

                int[] before_counts = new int[clusters_with_klome.size()];

                for (int i = 0; i < clusters_with_klome.size(); i++){
                    Triple<Integer, Double, Double> t1 = clusters_with_klome.get(i);
                    for (int j = 0; j < clusters_with_klome.size(); j++){
                        Triple<Integer, Double, Double> t2 = clusters_with_klome.get(j);
                        if (t1.getRight() < t2.getMiddle()){
                            before_counts[j] += 1;
                        }
                    }
                }

                for (int i = 0; i < before_counts.length; i++) {
                    if (before_counts[i] < N){
                        int cluster = clusters_with_klome.get(i).getLeft();

                        // System.out.println("TOPNCluster: " + cluster);
                        this.context.forward(cluster, this.clusterWithDensities.get(cluster));
                    }
                }

                this.context.forward(key, value);

                /* for(KeyValueIterator<Integer, Cluster> i = this.topNClusters.all(); i.hasNext();) {
                    KeyValue<Integer, Cluster> cluster = i.next();

                    this.topNClusters.delete(cluster.key);
                }*/

                /*for (Triple<Integer, Double, Double> t : queue) {
                    System.out.println("TOPNCluster: " + t.getLeft());
                    this.context.forward(t.getLeft(), this.clusterWithDensities.get(t.getLeft()));
                    // this.topNClusters.put(t.getLeft(), this.clusterWithDensities.get(t.getLeft()));
                }*/

                for(KeyValueIterator<Integer, Cluster> i = this.clusterWithDensities.all(); i.hasNext();) {
                    KeyValue<Integer, Cluster> cluster = i.next();

                    this.clusterWithDensities.delete(cluster.key);
                }

                context.commit();

                if(benchmarkTime == 0) {
                    benchmarkTime = System.currentTimeMillis() - start;
                } else {
                    benchmarkTime = (benchmarks * benchmarkTime + (System.currentTimeMillis() - start)) / (benchmarks + 1);
                }

                benchmarks++;

                System.out.println("Pruning Cluster: " + benchmarkTime);
            }
            else {
                this.clusterWithDensities.put(key, value);
            }
        }

        @Override
        public void close() { }
    }

    @Override
    public Processor<Integer, Cluster> get() {
        return new PruningProcessor(this.densityStoreName, this.topNStoreName);
    }
}
