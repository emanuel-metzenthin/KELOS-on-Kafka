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
import java.util.HashMap;

import static KELOS.Main.WINDOW_TIME;

public class KNearestClusterProcessorSupplier implements ProcessorSupplier<Integer, Cluster> {
    /*
        Finds the K nearest neighbors for each input Cluster.
     */
    private String storeName;

    public KNearestClusterProcessorSupplier(String storeName){
        this.storeName = storeName;
    }

    private class KNearestClusterProcessor implements Processor<Integer, Cluster> {
        private ProcessorContext context;
        private KeyValueStore<Integer, Cluster> clusters;
        String storeName;

        KNearestClusterProcessor(String storeName){
            this.storeName = storeName;
        }

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            this.clusters = (KeyValueStore<Integer, Cluster>) context.getStateStore(this.storeName);

            this.context.schedule(WINDOW_TIME, PunctuationType.STREAM_TIME, timestamp -> {
                /*
                HashMap<Integer, Cluster> uniqueClusters = new HashMap<>();
                for(KeyValueIterator<Windowed<Integer>, Cluster> i = this.clusters.all(); i.hasNext();) {
                    KeyValue<Windowed<Integer>, Cluster> cluster = i.next();
                    uniqueClusters.put(cluster.key.key(), cluster.value);
                }
                */

                for (KeyValueIterator<Integer, Cluster> it = this.clusters.all(); it.hasNext();){
                    KeyValue<Integer, Cluster> kv = it.next();
                    Cluster cluster = kv.value;

                    cluster.calculateKNearestNeighbors(this.clusters.all());

                    context.forward(kv.key, cluster);
                    context.commit();
                }

                for (KeyValueIterator<Integer, Cluster> it = this.clusters.all(); it.hasNext();){
                    KeyValue<Integer, Cluster> kv = it.next();
                    this.clusters.delete(kv.key);
                }
            });
        }

        @Override
        public void process(Integer key, Cluster cluster) {
            this.clusters.put(key, cluster);
        }

        @Override
        public void close() {

        }
    }

    @Override
    public Processor<Integer, Cluster> get() {
        return new KNearestClusterProcessor(this.storeName);
    }
}