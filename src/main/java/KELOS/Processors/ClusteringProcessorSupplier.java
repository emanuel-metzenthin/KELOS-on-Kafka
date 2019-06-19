package KELOS.Processors;

import KELOS.Cluster;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;

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
            private KeyValueStore<Integer, Cluster> tempClusters;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                this.tempClusters = (KeyValueStore<Integer, Cluster>) context.getStateStore("TempClusters");
                KeyValueStore<Integer, Cluster> clusters = (KeyValueStore<Integer, Cluster>) context.getStateStore("Clusters");

                // Emit cluster meta data after sub-window has been processed
                this.context.schedule(WINDOW_TIME, PunctuationType.STREAM_TIME, timestamp -> {
                    for(KeyValueIterator<Integer, Cluster> i = this.tempClusters.all(); i.hasNext();) {
                        KeyValue<Integer, Cluster> cluster = i.next();
                        context.forward(cluster.key, cluster.value, To.child("AggregationProcessor"));
                    }

                    context.commit();

                    // Clear all meta data in cluster store, but keep centroids for distance computation
                    for(KeyValueIterator<Integer, Cluster> i = clusters.all(); i.hasNext();) {
                        KeyValue<Integer, Cluster> cluster = i.next();
                        Cluster emptyCluster = new Cluster(cluster.value.centroid.length, K);
                        emptyCluster.centroid = cluster.value.centroid;

                        this.tempClusters.put(cluster.key, emptyCluster);
                    }
                });
            }

            /*
                Clusters data points by computing distances to cluster centroids
                and adding the points to the nearest cluster or by creating
                new clusters for distances above the threshold.
            */
            @Override
            public void process(Integer key, ArrayList<Double> value) {

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
                    cluster.addRecord(value);
                    this.tempClusters.put(clusterIdx, cluster);
                    this.context.forward(clusterIdx, value, To.child("ClusterAssignmentSink"));
                } else {
                    this.tempClusters.put(highestCluster + 1, new Cluster(value, K));
                    this.context.forward(highestCluster + 1, value, To.child("ClusterAssignmentSink"));
                }
            }

            @Override
            public void close() { }
        };
    }
}
