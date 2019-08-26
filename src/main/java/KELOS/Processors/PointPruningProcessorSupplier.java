package KELOS.Processors;

import KELOS.Cluster;
import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

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
        Calculates the KLOME-score of each candidate point
     */
    @Override
    public Processor<Integer, Pair<Cluster, Integer>> get() {

        return new Processor<Integer, Pair<Cluster, Integer>>() {
            private ProcessorContext context;
            private KeyValueStore<Integer, Pair<Cluster, Integer>> pointWithDensities;
            private long benchmarkTime = 0;
            private int benchmarks = 0;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                this.pointWithDensities = (KeyValueStore<Integer, Pair<Cluster, Integer>>) context.getStateStore("PointsWithDensities");
            }

            /*

             */
            @Override
            public void process(Integer key, Pair<Cluster, Integer> value) {

                if (Cluster.isEndOfWindowToken(value.getLeft())){
                    Date date = new Date(this.context.timestamp());
                    DateFormat formatter = new SimpleDateFormat("HH:mm:ss.SSS");
                    formatter.setTimeZone(TimeZone.getTimeZone("Europe/Berlin"));
                    String dateFormatted = formatter.format(date);
                    String systime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS"));

                    System.out.println("New Outlier window: " + dateFormatted + " System time : " + systime);

                    long start = System.currentTimeMillis();
                    MinMaxPriorityQueue<Pair<Integer, Double>> queue = MinMaxPriorityQueue
                            .orderedBy(new KlomeComparator())
                            .maximumSize(N)
                            .create();

                    boolean first = true;

                    for(KeyValueIterator<Integer, Pair<Cluster, Integer>> i = this.pointWithDensities.all(); i.hasNext();) {
                        KeyValue<Integer, Pair<Cluster, Integer>> point = i.next();


                        //No KLOME score calculation for non-candidates
                        if (point.value.getRight() == 1){
                            continue;
                        }
                        if(first) {
                            System.out.println("Outlier points from " + point.key);
                            first = false;
                        }
                        if(!i.hasNext()) {
                            System.out.println("Outlier points last " + point.key);
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

                        ArrayList<Double> pointArrayList = new ArrayList<>();
                        for(int k=0; k<point.value.getLeft().centroid.length; k++) {
                            pointArrayList.add(point.value.getLeft().centroid[k]);
                        }

                        Pair pointWithKlome = Pair.of(point.key, klome);

                        queue.add(pointWithKlome);
                    }


                    int count = 1;
                    // TODO: We eventually need to find a better solution, because the iterator doesn't preserve the order
                    for (Pair<Integer, Double> t : queue) {
                        int key2 = t.getLeft();
                        Cluster cluster = this.pointWithDensities.get(key2).getLeft();
                        this.context.forward(key2, cluster);
                        System.out.println("Outlier: " + count + " Punkt: " + key2 + " KLOME: " + t.getRight());
                        count++;
                    }

                    context.commit();

                    for(KeyValueIterator<Integer, Pair<Cluster, Integer>> i = this.pointWithDensities.all(); i.hasNext();) {
                        KeyValue<Integer, Pair<Cluster, Integer>> cluster = i.next();

                        this.pointWithDensities.delete(cluster.key);
                    }

                    if(benchmarkTime == 0) {
                        benchmarkTime = System.currentTimeMillis() - start;
                    } else {
                        benchmarkTime = (benchmarks * benchmarkTime + (System.currentTimeMillis() - start)) / (benchmarks + 1);
                    }

                    benchmarks++;

                    System.out.println("Prune Point: " + benchmarkTime);
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
