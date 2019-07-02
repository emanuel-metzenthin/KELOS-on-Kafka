package KELOS.Processors;

import KELOS.Cluster;
import KELOS.Main;
import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.*;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;


import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;

import static KELOS.Main.*;


public class FilterProcessorSupplier implements ProcessorSupplier<Integer, ArrayList<Double>> {
    /*

     */
    @Override
    public Processor<Integer, ArrayList<Double>> get() {
        return new Processor<Integer, ArrayList<Double>>() {
            private ProcessorContext context;
            private KeyValueStore<Integer, Cluster> topNClusters;
            private KeyValueStore<Integer, ArrayList<Double>> windowPoints;


            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                this.topNClusters = (KeyValueStore<Integer, Cluster>) context.getStateStore("TopNClusters");
                this.windowPoints = (KeyValueStore<Integer, ArrayList<Double>>) context.getStateStore("ClusterAssignments");

                this.context.schedule(WINDOW_TIME, PunctuationType.STREAM_TIME, timestamp -> {
                    for(KeyValueIterator<Integer, Cluster> i = this.topNClusters.all(); i.hasNext();) {
                        KeyValue<Integer, Cluster> cluster = i.next();

                        System.out.println("Cluster: " + cluster.key);
                    }

                    for(KeyValueIterator<Integer, ArrayList<Double>> i = this.windowPoints.all(); i.hasNext();) {
                        KeyValue<Integer, ArrayList<Double>> point = i.next();

                        Cluster cluster = this.topNClusters.get(point.key);

                        if (cluster != null){
                            // Workaround to reuse densityEstimator
                            Cluster singlePointCluster = new Cluster((ArrayList<Double>) point.value.subList(1, point.value.size()), K);
                            this.context.forward((int) (double) point.value.get(0), singlePointCluster);
                            System.out.println(point.key);
                        }

                        this.windowPoints.delete(point.key);
                    }
                });
            }

            /*

             */
            @Override
            public void process(Integer key, ArrayList<Double> value) {
                this.windowPoints.put(key, value);
            }

            @Override
            public void close() { }
        };
    }
}
