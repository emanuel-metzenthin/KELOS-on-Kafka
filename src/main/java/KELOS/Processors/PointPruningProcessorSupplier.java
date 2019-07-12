package KELOS.Processors;

import KELOS.Cluster;
import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;

import static KELOS.Main.*;


public class PointPruningProcessorSupplier implements ProcessorSupplier<Integer, Pair<Cluster, Integer>> {

    private class KlomeComparator implements Comparator<Pair<Integer, Double>> {
        @Override
        public int compare(Pair<Integer, Double> val1, Pair<Integer, Double> val2)
        {
            return val1.getRight().compareTo(val2.getRight());
        }
    }

    /*
        Calculates the KLOME_ score of each point
     */
    @Override
    public Processor<Integer, Pair<Cluster, Integer>> get() {

        return new Processor<Integer, Pair<Cluster, Integer>>() {
            private ProcessorContext context;
            private KeyValueStore<Integer, Pair<Cluster, Integer>> pointWithDensities;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                this.pointWithDensities = (KeyValueStore<Integer, Pair<Cluster, Integer>>) context.getStateStore("PointsWithDensities");

                this.context.schedule(WINDOW_TIME, PunctuationType.STREAM_TIME, timestamp -> {
                    MinMaxPriorityQueue<Pair<Integer, Double>> queue = MinMaxPriorityQueue
                            .orderedBy(new KlomeComparator())
                            .maximumSize(N)
                            .create();

                    for(KeyValueIterator<Integer, Pair<Cluster, Integer>> i = this.pointWithDensities.all(); i.hasNext();) {
                        KeyValue<Integer, Pair<Cluster, Integer>> point = i.next();

                        //No KLOME score calculation for non-candidates
                        if (point.value.getRight() == 1){
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

                        double klome = (point.value.getLeft().density - knnMean) / knnStddev;

                        ArrayList<Double> pointArrayList = new ArrayList<>();
                        for(int k=0; k<point.value.getLeft().centroid.length; k++) {
                            pointArrayList.add(point.value.getLeft().centroid[k]);
                        }

                        // Pair pointWithKlome = Pair.of(pointArrayList, klome);
                        Pair pointWithKlome = Pair.of(point.key, klome);

                        queue.add(pointWithKlome);
                    }


                    for (Pair<Integer, Double> t : queue) {
                        this.context.forward(t.getLeft(), t.getRight());
                        System.out.println("Outlier: " + t.getLeft() + " KLOME: " + t.getRight());
                    }

                    context.commit();

                    for(KeyValueIterator<Integer, Pair<Cluster, Integer>> i = this.pointWithDensities.all(); i.hasNext();) {
                        KeyValue<Integer, Pair<Cluster, Integer>> cluster = i.next();

                        this.pointWithDensities.delete(cluster.key);
                    }
                });
            }

            /*

             */
            @Override
            public void process(Integer key, Pair<Cluster, Integer> value) {
                System.out.println("Put " + key);
                this.pointWithDensities.put(key, value);
            }

            @Override
            public void close() { }
        };
    }
}
