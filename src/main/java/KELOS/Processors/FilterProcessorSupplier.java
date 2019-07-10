package KELOS.Processors;

import KELOS.Cluster;
import KELOS.Main;
import com.google.common.collect.MinMaxPriorityQueue;
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

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                this.topNClusters = (KeyValueStore<Integer, Cluster>) context.getStateStore("TopNClusters");
                this.windowPoints = (KeyValueStore<Integer, Triple<Integer, ArrayList<Double>, Long>>) context.getStateStore("ClusterAssignments");

                this.context.schedule(WINDOW_TIME, PunctuationType.STREAM_TIME, timestamp -> {
                    Date date = new Date(timestamp);
                    DateFormat formatter = new SimpleDateFormat("HH:mm:ss.SSS");
                    formatter.setTimeZone(TimeZone.getTimeZone("Europe/Berlin"));
                    String dateFormatted = formatter.format(date);
                    String systime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS"));

                    System.out.println("New FILTER window: " + dateFormatted + " System time : " + systime);

                    for(KeyValueIterator<Integer, Cluster> i = this.topNClusters.all(); i.hasNext();) {
                        KeyValue<Integer, Cluster> cluster = i.next();

                        System.out.println("TOP-N-Cluster: " + cluster.key);
                    }

                    // System.out.println("New Window");

                    for(KeyValueIterator<Integer,Triple<Integer, ArrayList<Double>, Long>> i = this.windowPoints.all(); i.hasNext();) {
                        KeyValue<Integer, Triple<Integer, ArrayList<Double>, Long>> point = i.next();

                        if (point.value.getRight() < timestamp - WINDOW_TIME.toMillis()){
                            Cluster cluster = this.topNClusters.get(point.value.getLeft());

                            // Workaround to reuse densityEstimator
                            Cluster singlePointCluster = new Cluster(point.value.getMiddle(), K);

                            if (cluster != null){
                                Pair<Cluster, Boolean> pair = Pair.of(singlePointCluster, true);
                                this.context.forward(point.key, pair);
                                System.out.println("Filter: " + point.key);
                            }
                            else {
                                Pair<Cluster, Boolean> pair = Pair.of(singlePointCluster, false);
                                this.context.forward(point.key, pair);
                            }

                            this.windowPoints.delete(point.key);
                        }
                    }

                    for(KeyValueIterator<Integer, Cluster> i = this.topNClusters.all(); i.hasNext();) {
                        KeyValue<Integer, Cluster> cluster = i.next();

                        this.topNClusters.delete(cluster.key);
                    }
                });
            }

            /*

             */
            @Override
            public void process(Integer key, Cluster value) {
                this.topNClusters.put(key, value);
            }

            @Override
            public void close() { }
        };
    }
}
