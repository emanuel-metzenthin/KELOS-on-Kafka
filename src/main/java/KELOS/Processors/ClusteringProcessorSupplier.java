package KELOS.Processors;

import KELOS.Cluster;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import java.util.ArrayList;

import static KELOS.Main.*;

public class ClusteringProcessorSupplier implements ProcessorSupplier<Integer, ArrayList<Double>> {

    /*
        Clusters data points in sub-window panes and emits cluster meta-data for the ClusterRegistrationProcessor
        to store into the global store. Assigns a point to its nearest cluster if the distance is below
        the threshold, otherwise it will create a new cluster.
        These panes then get merged to whole windows by the following AggregationProcessor.
     */
    @Override
    public Processor<Integer, ArrayList<Double>> get() {
        return new Processor<Integer, ArrayList<Double>>() {
            private ProcessorContext context;
            private KeyValueStore<Integer, ArrayList<Double>> pointBuffer;
            private KeyValueStore<Integer, Cluster> tempClusters;
            private WindowStore<Integer, Triple<Integer, ArrayList<Double>, Long>> clusterAssignments;
            private KeyValueStore<Integer, Cluster> clusters;

            /*
                Assigns an incoming point to a cluster or creates a new cluster.
                The ArrayList value contains the individual coordinates of the point.
             */
            private void processPoint(Integer key, ArrayList<Double> value){
                // Find nearest cluster
                double minDist = Double.MAX_VALUE;
                Cluster cluster = null;
                int clusterIdx = 0;
                int highestClusterIdx = 0; // Keep highest index value for creating new clusters

                for(KeyValueIterator<Integer, Cluster> i = this.tempClusters.all(); i.hasNext();) {
                    KeyValue<Integer, Cluster> tmpCluster = i.next();

                    double dist = tmpCluster.value.distance(value);

                    if (dist < minDist) {
                        minDist = dist;
                        cluster = tmpCluster.value;
                        clusterIdx = tmpCluster.key;
                    }

                    highestClusterIdx = Math.max(highestClusterIdx, tmpCluster.key);
                }

                if (minDist < CLUSTERING_DISTANCE_THRESHOLD) {
                    // Create cluster assignment triple
                    Triple<Integer, ArrayList<Double>, Long> triple = Triple.of(clusterIdx, value, this.context.timestamp());
                    this.clusterAssignments.put(key, triple);

                    cluster.addRecord(value);
                    this.tempClusters.put(clusterIdx, cluster);

                    this.context.forward(key, triple, To.child("ClusterAssignmentSink"));
                } else {
                    // Create cluster assignment triple for new cluster
                    Triple<Integer, ArrayList<Double>, Long> triple = Triple.of(highestClusterIdx + 1, value, this.context.timestamp());
                    this.clusterAssignments.put(key, triple);

                    this.tempClusters.put(highestClusterIdx + 1, new Cluster(value, K));

                    this.context.forward(key, triple, To.child("ClusterAssignmentSink"));
                }
            }

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                this.pointBuffer = (KeyValueStore<Integer, ArrayList<Double>>) context.getStateStore("ClusteringBuffer");
                this.tempClusters = (KeyValueStore<Integer, Cluster>) context.getStateStore("TempClusters");
                this.clusterAssignments = (WindowStore<Integer, Triple<Integer, ArrayList<Double>, Long>>) context.getStateStore("ClusterAssignments");
                this.clusters = (KeyValueStore<Integer, Cluster>) context.getStateStore("Clusters");

                // Emit cluster meta data after sub-window has been processed
                this.context.schedule(WINDOW_TIME, PunctuationType.STREAM_TIME, timestamp -> {
                    // Perform clustering
                    for(KeyValueIterator<Integer, ArrayList<Double>> i = this.pointBuffer.all(); i.hasNext();) {
                        KeyValue<Integer, ArrayList<Double>> point = i.next();

                        this.processPoint(point.key, point.value);

                        this.pointBuffer.delete(point.key);
                    }


                    for(KeyValueIterator<Integer, Cluster> i = this.tempClusters.all(); i.hasNext();) {
                        KeyValue<Integer, Cluster> cluster = i.next();
                        cluster.value.updateMetrics();

                        context.forward(cluster.key, cluster.value, To.child("AggregationProcessor"));

                        this.tempClusters.delete(cluster.key);
                    }

                    // Forward end of window token
                    context.forward(-1, Cluster.createEndOfWindowToken(), To.child("AggregationProcessor"));

                    context.commit();

                    // Initialize cluster with old metrics for stability in next sub-window pane
                    for(KeyValueIterator<Integer, Cluster> i = clusters.all(); i.hasNext();) {
                        KeyValue<Integer, Cluster> cluster = i.next();
                        Cluster emptyCluster = new Cluster(cluster.value, K);
                        emptyCluster.centroid = cluster.value.centroid;

                        this.tempClusters.put(cluster.key, emptyCluster);
                    }
                });
            }

            @Override
            public void process(Integer key, ArrayList<Double> value) {
               this.pointBuffer.put(key, value);
            }

            @Override
            public void close() { }
        };
    }
}
