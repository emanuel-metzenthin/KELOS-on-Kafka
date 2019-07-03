package KELOS.Processors;

import KELOS.Cluster;
import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;


import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

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

        PruningProcessor(String densityStoreName, String topNStoreName){
            this.densityStoreName = densityStoreName;
            this.topNStoreName = topNStoreName;
        }

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            this.clusterWithDensities = (KeyValueStore<Integer, Cluster>) context.getStateStore(this.densityStoreName);
            this.topNClusters = (KeyValueStore<Integer, Cluster>) context.getStateStore(this.topNStoreName);


            this.context.schedule(WINDOW_TIME, PunctuationType.STREAM_TIME, timestamp -> {
                MinMaxPriorityQueue<Triple<Integer, Double, Double>> queue = MinMaxPriorityQueue
                        .orderedBy(new KlomeComparator())
                        .maximumSize(N)
                        .create();


                for(KeyValueIterator<Integer, Cluster> i = this.clusterWithDensities.all(); i.hasNext();) {
                    KeyValue<Integer, Cluster> cluster = i.next();

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

                    if (queue.size() < N){
                        queue.add(triple);
                    }
                    else if (klomeHigh < queue.peek().getMiddle()){
                        queue.poll();
                        queue.add(triple);
                    }
                    else if (klomeLow <= queue.peek().getRight()){
                        queue.add(triple);
                    }
                }

                for(KeyValueIterator<Integer, Cluster> i = this.topNClusters.all(); i.hasNext();) {
                    KeyValue<Integer, Cluster> cluster = i.next();

                    this.topNClusters.delete(cluster.key);
                }

                for (Triple<Integer, Double, Double> t : queue) {
                    this.topNClusters.put(t.getLeft(), this.clusterWithDensities.get(t.getLeft()));
                }

                for(KeyValueIterator<Integer, Cluster> i = this.clusterWithDensities.all(); i.hasNext();) {
                    KeyValue<Integer, Cluster> cluster = i.next();

                    this.clusterWithDensities.delete(cluster.key);
                }

                context.commit();
            });
        }

        /*

         */
        @Override
        public void process(Integer key, Cluster value) {
            this.clusterWithDensities.put(key, value);
        }

        @Override
        public void close() { }
    }

    @Override
    public Processor<Integer, Cluster> get() {
        return new PruningProcessor(this.densityStoreName, this.topNStoreName);
    }
}
