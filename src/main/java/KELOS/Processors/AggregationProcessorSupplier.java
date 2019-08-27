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
                // Finish window processing if message is EndOfWindowToken
                if (Cluster.isEndOfWindowToken(value)){
                    this.context.forward(key, value);

                    return;
                }

                // Get list of previous states of the cluster at hand
                ArrayList<Cluster> previousStates = this.clusterStates.get(key);

                if (previousStates == null || previousStates.size() == 0 || previousStates.get(0) == null) {
                    // Avoid adding old empty clusters again
                    if (value.size > 0){
                        ArrayList<Cluster> futureStates = new ArrayList<>();
                        futureStates.add(value);

                        this.context.forward(key, value);
                        this.clusterStates.put(key, futureStates);
                    }
                } else {
                    ArrayList<Cluster> futureStates = previousStates;

                    // Limit maximum sub-window pane count
                    if (previousStates.size() >= Main.AGGREGATION_WINDOWS){
                        futureStates.remove(0);
                    }

                    // Merge cluster states
                    Cluster aggregate = new Cluster(value.centroid.length, K);
                    aggregate.merge(value);

                    for (Cluster c : futureStates){
                        aggregate.merge(c);
                    }

                    futureStates.add(value);

                    if (aggregate.size == 0){
                        // Delete empty clusters
                        this.context.forward(key, null);
                    }
                    else {
                        this.context.forward(key, aggregate);
                    }

                    this.clusterStates.put(key, futureStates);
                }
            }

            @Override
            public void close() { }
        };
    }
}