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


public class PointPruningProcessorSupplier implements ProcessorSupplier<Integer, Cluster> {

    private class KlomeComparator implements Comparator<Pair<ArrayList<Double>, Double>> {
        @Override
        public int compare(Pair<ArrayList<Double>, Double> val1, Pair<ArrayList<Double>, Double> val2)
        {
            return val1.getRight().compareTo(val2.getRight());
        }
    }

    /*
        Calculates the KLOME_ score of each point
     */
    @Override
    public Processor<Integer, Cluster> get() {

        return new Processor<Integer, Cluster>() {
            private ProcessorContext context;
            private KeyValueStore<Integer, Cluster> pointWithDensities;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                this.pointWithDensities = (KeyValueStore<Integer, Cluster>) context.getStateStore("PointsWithDensities");

                this.context.schedule(WINDOW_TIME, PunctuationType.STREAM_TIME, timestamp -> {
                    MinMaxPriorityQueue<Pair<ArrayList<Double>, Double>> queue = MinMaxPriorityQueue
                            .orderedBy(new KlomeComparator())
                            .maximumSize(N)
                            .create();

                    for(KeyValueIterator<Integer, Cluster> i = this.pointWithDensities.all(); i.hasNext();) {
                        KeyValue<Integer, Cluster> point = i.next();

                        double knnMean = 0;
                        double knnVariance = 0;

                        ArrayList<Integer> existingKnnIds = new ArrayList<>();

                        for (int id : point.value.knnIds){
                            if (this.pointWithDensities.get(id) != null) {
                                existingKnnIds.add(id);
                                knnMean += this.pointWithDensities.get(id).density;
                            }
                        }

                        knnMean /= existingKnnIds.size();

                        for (int id : existingKnnIds){
                            knnVariance += Math.pow((this.pointWithDensities.get(id).density - knnMean), 2);
                        }

                        double knnStddev = Math.sqrt(knnVariance);

                        double klome = (point.value.minDensityBound - knnMean) / knnStddev;

                        ArrayList<Double> pointArrayList = new ArrayList<>();
                        for(int k=0; k<point.value.centroid.length; k++) {
                            pointArrayList.add(point.value.centroid[k]);
                        }

                        Pair pointWithKlome = Pair.of(pointArrayList, klome);

                        queue.add(pointWithKlome);
                    }


                    for (Pair<ArrayList<Double>, Double> t : queue) {
                        this.context.forward(t.getLeft(), t.getRight());
                        System.out.println("Outlier: " + t.getRight());
                    }

                    context.commit();
                });
            }

            /*

             */
            @Override
            public void process(Integer key, Cluster value) {
                this.pointWithDensities.put(key, value);
            }

            @Override
            public void close() { }
        };
    }
}
