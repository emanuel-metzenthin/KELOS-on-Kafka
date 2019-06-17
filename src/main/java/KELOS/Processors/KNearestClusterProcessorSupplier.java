package KELOS.Processors;

import KELOS.Cluster;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;

import static KELOS.Main.WINDOW_TIME;

public class KNearestClusterProcessorSupplier implements ProcessorSupplier<Integer, Cluster> {
    /*
        Finds the K nearest neighbors for each input Cluster.
     */
    @Override
    public Processor<Integer, Cluster> get() {
        return new Processor<Integer, Cluster>() {
            private ProcessorContext context;
            private WindowStore<Integer, Cluster> clusters;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                this.clusters = (WindowStore<Integer, Cluster>) context.getStateStore("ClusterBuffer");

                this.context.schedule(WINDOW_TIME, PunctuationType.STREAM_TIME, timestamp -> {
                    for(KeyValueIterator<Windowed<Integer>, Cluster> i = this.clusters.fetchAll(timestamp - WINDOW_TIME.toMillis() , timestamp); i.hasNext();) {
                        KeyValue<Windowed<Integer>, Cluster> cluster = i.next();

                        cluster.value.calculateKNearestNeighbors(this.clusters.fetchAll(timestamp - WINDOW_TIME.toMillis() , timestamp));

                        context.forward(cluster.key, cluster.value);
                    }

                    context.commit();
                });
            }

            @Override
            public void process(Integer key, Cluster value) {
                this.clusters.put(key, value);
            }

            @Override
            public void close() { }
        };
    }
}