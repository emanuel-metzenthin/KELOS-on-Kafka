package KELOS.Processors;

import KELOS.Cluster;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

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
                this.clusters = (KeyValueStore<Integer, Cluster>) context.getStateStore("ClusterBuffer");
            }

            @Override
            public void process(Integer key, Cluster value) {
                if (Cluster.isEndOfWindowToken(value)){
                    for (KeyValueIterator<Integer, Cluster> it = this.clusters.all(); it.hasNext(); ) {
                        KeyValue<Integer, Cluster> kv = it.next();
                        Cluster cluster = kv.value;

                        cluster.calculateKNearestNeighbors(this.clusters.all(), kv.key);

                        context.forward(kv.key, cluster);
                        context.commit();
                    }

                    // Forward EndOfWindowToken
                    context.forward(key, value);

                    for (KeyValueIterator<Integer, Cluster> it = this.clusters.all(); it.hasNext(); ) {
                        KeyValue<Integer, Cluster> kv = it.next();
                        this.clusters.delete(kv.key);
                    }
                } else {
                    this.clusters.put(key, value);
                }
            }

            @Override
            public void close() {

            }
        };
    }
}