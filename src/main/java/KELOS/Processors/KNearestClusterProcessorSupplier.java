package KELOS.Processors;

import KELOS.Cluster;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import java.util.ArrayList;

import static KELOS.Main.WINDOW_TIME;

public class KNearestClusterProcessorSupplier implements ProcessorSupplier<Integer, Cluster> {
    /*
        Finds the K nearest neighbors for each input Cluster.
     */
    @Override
    public Processor<Integer, Cluster> get() {
        return new Processor<Integer, Cluster>() {
            private ProcessorContext context;
            private KeyValueStore<Integer, Cluster> clusters;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                this.clusters = (KeyValueStore<Integer, Cluster>) context.getStateStore("Clusters");

                this.context.schedule(WINDOW_TIME, PunctuationType.STREAM_TIME, timestamp -> {
                    ArrayList<KeyValue<Integer, Cluster>> clusterList = new ArrayList<>();

                    for(KeyValueIterator<Integer, Cluster> i = this.clusters.all(); i.hasNext();) {
                        clusterList.add(i.next());
                    }

                    for(KeyValue<Integer, Cluster> cluster : clusterList) {
                        cluster.value.calculateKNearestNeighbors(clusterList);

                        context.forward(cluster.key, cluster.value);
                    }

                    context.commit();
                });
            }

            @Override
            public void process(Integer key, Cluster value) {

            }

            @Override
            public void close() { }
        };
    }
}