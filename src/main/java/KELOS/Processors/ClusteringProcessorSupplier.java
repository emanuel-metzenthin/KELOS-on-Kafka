package KELOS.Processors;

import KELOS.Cluster;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.TimeZone;

import static KELOS.Main.*;

public class ClusteringProcessorSupplier implements ProcessorSupplier<Integer, ArrayList<Double>> {

    /*
        Clusters data points in sub-windows and emits cluster meta-data for the ClusterRegistrationProcessor
        to aggregate them into the global store.
     */
    @Override
    public Processor<Integer, ArrayList<Double>> get() {
        return new Processor<Integer, ArrayList<Double>>() {
            private ProcessorContext context;
            private KeyValueStore<Integer, ArrayList<Double>> pointBuffer;
            private KeyValueStore<Integer, Cluster> tempClusters;
            private KeyValueStore<Integer, Triple<Integer, ArrayList<Double>, Long>> clusterAssignments;
            private long benchmarkTime = 0;
            private int benchmarks = 0;

            private void processPoint(Integer key, ArrayList<Double> value){

                double minDist = Double.MAX_VALUE;
                Cluster cluster = null;
                int clusterIdx = 0;
                int highestCluster = 0; // Highest cluster index, needed to create new clusters

                for(KeyValueIterator<Integer, Cluster> i = this.tempClusters.all(); i.hasNext();) {
                    KeyValue<Integer, Cluster> c = i.next();

                    double dist = c.value.distance(value);

                    if (dist < minDist) {
                        minDist = dist;
                        cluster = c.value;
                        clusterIdx = c.key;
                    }

                    highestCluster = Math.max(highestCluster, c.key);
                }

                if (minDist < DISTANCE_THRESHOLD) {
                    Triple<Integer, ArrayList<Double>, Long> triple = Triple.of(clusterIdx, value, this.context.timestamp());
                    cluster.addRecord(value);
                    this.tempClusters.put(clusterIdx, cluster);
                    this.clusterAssignments.put(key, triple);

                    this.context.forward(key, triple, To.child("ClusterAssignmentSink"));
                } else {
                    Triple<Integer, ArrayList<Double>, Long> triple = Triple.of(highestCluster + 1, value, this.context.timestamp());
                    this.tempClusters.put(highestCluster + 1, new Cluster(value, K));

                    this.clusterAssignments.put(key, triple);
                    this.context.forward(key, triple, To.child("ClusterAssignmentSink"));
                }
            }

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                this.pointBuffer = (KeyValueStore<Integer, ArrayList<Double>>) context.getStateStore("ClusteringBuffer");
                this.tempClusters = (KeyValueStore<Integer, Cluster>) context.getStateStore("TempClusters");
                this.clusterAssignments = (KeyValueStore<Integer, Triple<Integer, ArrayList<Double>, Long>>) context.getStateStore("ClusterAssignments");

                KeyValueStore<Integer, Cluster> clusters = (KeyValueStore<Integer, Cluster>) context.getStateStore("Clusters");

                // Emit cluster meta data after sub-window has been processed
                this.context.schedule(WINDOW_TIME, PunctuationType.STREAM_TIME, timestamp -> {
                    Date date = new Date(timestamp);
                    DateFormat formatter = new SimpleDateFormat("HH:mm:ss.SSS");
                    formatter.setTimeZone(TimeZone.getTimeZone("Europe/Berlin"));
                    String dateFormatted = formatter.format(date);
                    String systime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS"));

                    System.out.println("New Clustering window: " + dateFormatted + " System time : " + systime);

                    long start = System.currentTimeMillis();

                    boolean first = true;

                    // Perform clustering
                    for(KeyValueIterator<Integer, ArrayList<Double>> i = this.pointBuffer.all(); i.hasNext();) {
                        KeyValue<Integer, ArrayList<Double>> point = i.next();

                        if(first) {
                            System.out.println("Clustering points from " + point.key);
                            first = false;
                        }
                        if(!i.hasNext()) {
                            System.out.println("Clustering points last " + point.key);
                        }

                        this.processPoint(point.key, point.value);

                        this.pointBuffer.delete(point.key);
                    }


                    for(KeyValueIterator<Integer, Cluster> i = this.tempClusters.all(); i.hasNext();) {
                        KeyValue<Integer, Cluster> cluster = i.next();
                        cluster.value.updateMetrics();
                        context.forward(cluster.key, cluster.value, To.child("AggregationProcessor"));

                        this.tempClusters.delete(cluster.key);
                    }

                    context.forward(-1, Cluster.createEndOfWindowToken(), To.child("AggregationProcessor"));

                    context.commit();

                    // Initialize cluster with old metrics for stability
                    for(KeyValueIterator<Integer, Cluster> i = clusters.all(); i.hasNext();) {
                        KeyValue<Integer, Cluster> cluster = i.next();
                        Cluster emptyCluster = new Cluster(cluster.value, K);
                        emptyCluster.centroid = cluster.value.centroid;

                        this.tempClusters.put(cluster.key, emptyCluster);
                    }



                    if(benchmarkTime == 0) {
                        benchmarkTime = System.currentTimeMillis() - start;
                    } else {
                        benchmarkTime = (benchmarks * benchmarkTime + (System.currentTimeMillis() - start)) / (benchmarks + 1);
                    }

                    benchmarks++;

                    System.out.println("Clustering: " + benchmarkTime);
                });
            }

            /*
                Clusters data points by computing distances to cluster centroids
                and adding the points to the nearest cluster or by creating
                new clusters for distances above the threshold.
            */
            @Override
            public void process(Integer key, ArrayList<Double> value) {

               this.pointBuffer.put(key, value);
            }

            @Override
            public void close() { }
        };
    }
}
