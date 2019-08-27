package KELOS.Processors;

import KELOS.Cluster;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

public class ClusterRegistrationProcessorSupplier implements ProcessorSupplier<Integer, Cluster> {

    /*
        Stores clusters in global cluster store
     */
    @Override
    public Processor<Integer, Cluster> get() {
        return new Processor<Integer, Cluster>() {
            private KeyValueStore<Integer, Cluster> state;

            @Override
            public void init(ProcessorContext context) {
                this.state = (KeyValueStore<Integer, Cluster>) context.getStateStore("Clusters");
            }

            @Override
            public void process(Integer key, Cluster value) {
                if (!Cluster.isEndOfWindowToken(value)){
                    this.state.put(key, value);
                }
            }

            @Override
            public void close() { }
        };
    }
}