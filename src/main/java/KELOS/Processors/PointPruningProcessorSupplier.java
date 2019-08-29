package KELOS.Processors;

import KELOS.Cluster;
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

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                this.pointWithDensities = (KeyValueStore<Integer, Pair<Cluster, Boolean>>) context.getStateStore("PointsWithDensities");
            }

            @Override
            public void process(Integer key, Pair<Cluster, Boolean> value) {
                if (Cluster.isEndOfWindowToken(value.getLeft())){
                    PriorityQueue<Pair<Integer, Double>> queue = new PriorityQueue<>(N, new KlomeComparator());

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

                        Pair<Integer, Double> pointWithKlome = Pair.of(point.key, klome);

                        queue.add(pointWithKlome);
                    }

                    for (int count = 0; count < N && !queue.isEmpty(); count++) {
                        Pair<Integer, Double> t = queue.poll();
                        int key2 = t.getLeft();
                        Cluster cluster = this.pointWithDensities.get(key2).getLeft();
                        this.context.forward(key2, cluster);
                        System.out.println("Outlier: " + count + " Punkt: " + key2 + " KLOME: " + t.getRight() + " density: " + cluster.density);
                    }

                    context.commit();

                    for(KeyValueIterator<Integer, Pair<Cluster, Boolean>> i = this.pointWithDensities.all(); i.hasNext();) {
                        KeyValue<Integer, Pair<Cluster, Boolean>> cluster = i.next();

                        this.pointWithDensities.delete(cluster.key);
                    }
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
