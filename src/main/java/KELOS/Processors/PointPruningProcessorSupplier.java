package KELOS.Processors;

import KELOS.Cluster;
import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import java.util.*;

import static KELOS.Main.*;


public class PointPruningProcessorSupplier implements ProcessorSupplier<Integer, Pair<Cluster, Boolean>> {

    private class KlomeComparator implements Comparator<Pair<Integer, Double>> {
        @Override
        public int compare(Pair<Integer, Double> val1, Pair<Integer, Double> val2)
        {
            return val1.getRight().compareTo(val2.getRight());
        }
    }

    /*
        Calculates the KLOME-score of each candidate point
     */
    @Override
    public Processor<Integer, Pair<Cluster, Boolean>> get() {

        return new Processor<Integer, Pair<Cluster, Boolean>>() {
            private ProcessorContext context;
            private KeyValueStore<Integer, Pair<Cluster, Boolean>> pointWithDensities;

            private long benchmarkTime = 0;
            private long benchmarks = 0;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                this.pointWithDensities = (KeyValueStore<Integer, Pair<Cluster, Boolean>>) context.getStateStore("PointsWithDensities");
            }

            @Override
            public void process(Integer key, Pair<Cluster, Boolean> value) {
                if (Cluster.isEndOfWindowToken(value.getLeft())){
                    long start = System.currentTimeMillis();
                    MinMaxPriorityQueue<Pair<Integer, Double>> queue = MinMaxPriorityQueue
                            .orderedBy(new KlomeComparator())
                            .maximumSize(N)
                            .create();

                    for(KeyValueIterator<Integer, Pair<Cluster, Boolean>> i = this.pointWithDensities.all(); i.hasNext();) {
                        KeyValue<Integer, Pair<Cluster, Boolean>> point = i.next();

                        //No KLOME score calculation for non-candidates
                        if (!point.value.getRight()){
                            continue;
                        }

                        double knnMean = 0;
                        double knnVariance = 0;

                        ArrayList<Integer> existingKnnIds = new ArrayList<>();

                        for (int id : point.value.getLeft().knnIds){
                            if (this.pointWithDensities.get(id) != null) {
                                existingKnnIds.add(id);
                                knnMean += this.pointWithDensities.get(id).getLeft().density;
                            }
                        }

                        knnMean /= existingKnnIds.size();

                        for (int id : existingKnnIds){
                            knnVariance += Math.pow((this.pointWithDensities.get(id).getLeft().density - knnMean), 2);
                        }

                        double knnStddev = Math.sqrt(knnVariance);

                        double klome;

                        // Can happen if all KNNs have the exact same coordinates as the point itself
                        if (knnStddev == 0){
                            klome = Double.MAX_VALUE;
                        }
                        else {
                            klome = (point.value.getLeft().density - knnMean) / knnStddev;
                        }

                        Pair pointWithKlome = Pair.of(point.key, klome);

                        queue.add(pointWithKlome);
                    }

                    int count = 1;

                    for (Pair<Integer, Double> t : queue) {
                        int key2 = t.getLeft();
                        Cluster cluster = this.pointWithDensities.get(key2).getLeft();
                        this.context.forward(key2, cluster);
                        System.out.println("Outlier: " + count + " Punkt: " + key2 + " KLOME: " + t.getRight() + " density: " + cluster.density);
                        count++;
                    }

                    context.commit();

                    for(KeyValueIterator<Integer, Pair<Cluster, Boolean>> i = this.pointWithDensities.all(); i.hasNext();) {
                        KeyValue<Integer, Pair<Cluster, Boolean>> cluster = i.next();

                        this.pointWithDensities.delete(cluster.key);
                    }

                    if(benchmarkTime == 0) {
                        benchmarkTime = System.currentTimeMillis() - start;
                    } else {
                        benchmarkTime = (benchmarks * benchmarkTime + (System.currentTimeMillis() - start)) / (benchmarks + 1);
                    }

                    benchmarks++;

                    System.out.println("Point Pruning: " + benchmarkTime);
                }
                else {
                    this.pointWithDensities.put(key, value);
                }
            }

            @Override
            public void close() { }
        };
    }
}
