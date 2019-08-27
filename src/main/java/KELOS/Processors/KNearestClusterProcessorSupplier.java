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

            private long benchmarkTime = 0;
            private int benchmarks = 0;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                this.clusters = (KeyValueStore<Integer, Cluster>) context.getStateStore("ClusterBuffer");
            }

            @Override
            public void process(Integer key, Cluster value) {
                if (Cluster.isEndOfWindowToken(value)){
                    long start = System.currentTimeMillis();

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

                    if(benchmarkTime == 0) {
                        benchmarkTime = System.currentTimeMillis() - start;
                    } else {
                        benchmarkTime = (benchmarks * benchmarkTime + (System.currentTimeMillis() - start)) / (benchmarks + 1);
                    }

                    benchmarks++;

                    System.out.println("KNN Cluster: " + benchmarkTime);
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