package KELOS.Processors;

import KELOS.Cluster;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
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


public class FilterProcessorSupplier implements ProcessorSupplier<Integer, Cluster> {
    /*

     */
    @Override
    public Processor<Integer, Cluster> get() {
        return new Processor<Integer, Cluster>() {
            private ProcessorContext context;
            private KeyValueStore<Integer, Cluster> topNClusters;
            private KeyValueStore<Integer, Triple<Integer, ArrayList<Double>, Long>> windowPoints;
            private long benchmarkTime = 0;
            private int benchmarks = 0;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                this.topNClusters = (KeyValueStore<Integer, Cluster>) context.getStateStore("TopNClusters");
                this.windowPoints = (KeyValueStore<Integer, Triple<Integer, ArrayList<Double>, Long>>) context.getStateStore("ClusterAssignments");

            }

            /*

             */
            @Override
            public void process(Integer key, Cluster value) {

                if (Cluster.isEndOfWindowToken(value)){
                    long start = System.currentTimeMillis();

                    Date date = new Date(this.context.timestamp());
                    DateFormat formatter = new SimpleDateFormat("HH:mm:ss.SSS");
                    formatter.setTimeZone(TimeZone.getTimeZone("Europe/Berlin"));
                    String dateFormatted = formatter.format(date);
                    String systime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS"));

                    System.out.println("New FILTER window: " + dateFormatted + " System time : " + systime);

                    boolean first = true;
                    for(KeyValueIterator<Integer,Triple<Integer, ArrayList<Double>, Long>> i = this.windowPoints.all(); i.hasNext();) {
                        KeyValue<Integer, Triple<Integer, ArrayList<Double>, Long>> point = i.next();

                        if (point.value.getRight() <= this.context.timestamp()){
                            if(first) {
                                System.out.println("Filter points from " + point.key);
                                first = false;
                            }
                            if(!i.hasNext()) {
                                System.out.println("Filter points last " + point.key);
                            }

                            Cluster cluster = this.topNClusters.get(point.value.getLeft());

                            // Workaround to reuse densityEstimator
                            Cluster singlePointCluster = new Cluster(point.value.getMiddle(), K);

                            if (cluster != null){
                                Pair<Cluster, Boolean> pair = Pair.of(singlePointCluster, true);
                                this.context.forward(point.key, pair);
                            }
                            else {
                                Pair<Cluster, Boolean> pair = Pair.of(singlePointCluster, false);
                                this.context.forward(point.key, pair);
                            }

                            this.windowPoints.delete(point.key);
                        }
                    }

                    this.context.forward(key, Pair.of(value, true));

                    for(KeyValueIterator<Integer, Cluster> i = this.topNClusters.all(); i.hasNext();) {
                        KeyValue<Integer, Cluster> cluster = i.next();

                        this.topNClusters.delete(cluster.key);
                    }

                    if(benchmarkTime == 0) {
                        benchmarkTime = System.currentTimeMillis() - start;
                    } else {
                        benchmarkTime = (benchmarks * benchmarkTime + (System.currentTimeMillis() - start)) / (benchmarks + 1);
                    }

                    benchmarks++;

                    System.out.println("Filter: " + benchmarkTime);
                }
                else {
                    this.topNClusters.put(key, value);
                }
            }

            @Override
            public void close() { }
        };
    }
}
