package KELOS.Processors;

import KELOS.Cluster;
import KELOS.Main;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;

import static KELOS.Main.K;

public class AggregationProcessorSupplier implements ProcessorSupplier<Integer, Cluster> {

    /*
        Aggregates the sub-windows and emits the aggregated clusters.
        Stores lists of the states of individual clusters in a local store,
        merges new arriving clusters with their old states.
     */
    @Override
    public Processor<Integer, Cluster> get() {
        return new Processor<Integer, Cluster>() {
            private ProcessorContext context;
            private KeyValueStore<Integer, ArrayList<Cluster>> clusterStates;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                this.clusterStates = (KeyValueStore<Integer, ArrayList<Cluster>>) context.getStateStore("ClusterStates");
            }

            @Override
            public void process(Integer key, Cluster value) {
                ArrayList<Cluster> oldList = this.clusterStates.get(key);

                if (oldList == null || oldList.size() == 0 || oldList.get(0) == null) {
                    if (value.size > 0){
                        ArrayList<Cluster> newList = new ArrayList<>();
                        newList.add(value);

                        this.context.forward(key, value);
                        this.clusterStates.put(key, newList);
                    }
                } else {
                    ArrayList<Cluster> newList = oldList;

                    if (oldList.size() >= Main.AGGREGATION_WINDOWS){
                        newList.remove(0);
                    }

                    Cluster aggregate = new Cluster(value.centroid.length, K);
                    aggregate.merge(value);

                    for (Cluster c : newList){
                        aggregate.merge(c);
                    }

                    newList.add(value);

                    if (aggregate.size == 0){
                        // System.out.println("Forward " + key + " Size = null");
                        this.context.forward(key, null); // Delete empty cluster
                    }
                    else {
                        // System.out.println("Forward " + key + " Size = " + aggregate.size);
                        this.context.forward(key, aggregate);
                    }


                    this.clusterStates.put(key, newList);
                }
            }

            @Override
            public void close() { }
        };
    }
}