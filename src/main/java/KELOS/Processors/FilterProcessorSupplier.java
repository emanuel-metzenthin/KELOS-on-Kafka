package KELOS.Processors;

import KELOS.Cluster;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;

import java.util.ArrayList;

import static KELOS.Main.*;


public class FilterProcessorSupplier implements ProcessorSupplier<Integer, Cluster> {
    /*
        Filters all points in window based on wether they belong to the clusters that could contain outliers.
     */
    @Override
    public Processor<Integer, Cluster> get() {
        return new Processor<Integer, Cluster>() {
            private ProcessorContext context;
            private KeyValueStore<Integer, Cluster> topNClusters;
            private WindowStore<Integer, Triple<Integer, ArrayList<Double>, Long>> windowPoints;

            private long benchmarkTime = 0;
            private int benchmarks = 0;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                this.topNClusters = (KeyValueStore<Integer, Cluster>) context.getStateStore("TopNClusters");
                this.windowPoints = (WindowStore<Integer, Triple<Integer, ArrayList<Double>, Long>>) context.getStateStore("ClusterAssignments");

            }

            @Override
            public void process(Integer key, Cluster value) {
                if (Cluster.isEndOfWindowToken(value)){
                    long start = System.currentTimeMillis();

                    // Fetch all points and their cluster assignment in current aggregated window
                    long fromTime = this.context.timestamp() - (long) (((double) AGGREGATION_WINDOWS - 0.5) * WINDOW_TIME.toMillis());
                    long toTime = this.context.timestamp();

                    for(KeyValueIterator<Windowed<Integer>, Triple<Integer, ArrayList<Double>, Long>> i = this.windowPoints.fetchAll(fromTime, toTime); i.hasNext();) {
                        KeyValue<Windowed<Integer>, Triple<Integer, ArrayList<Double>, Long>> point = i.next();

                        // Check if the point's cluster is one of the clusters that could contain outliers
                        Cluster cluster = this.topNClusters.get(point.value.getLeft());

                        // Create cluster representation of the point
                        Cluster singlePointCluster = new Cluster(point.value.getMiddle(), K);

                        if (cluster != null){
                            // Indicate the point is an outlier candidate
                            Pair<Cluster, Boolean> pair = Pair.of(singlePointCluster, true);
                            this.context.forward(point.key.key(), pair);
                        }
                        else {
                            // Indicate the point is NOT an outlier candidate
                            Pair<Cluster, Boolean> pair = Pair.of(singlePointCluster, false);
                            this.context.forward(point.key.key(), pair);
                        }
                    }

                    // Forward EndOfWindowToken
                    this.context.forward(key, Pair.of(value, true));

                    for(KeyValueIterator<Integer, Cluster> i = this.topNClusters.all(); i.hasNext();) {
                        KeyValue<Integer, Cluster> cluster = i.next();

                        this.topNClusters.delete(cluster.key);
                    }

                    if(benchmarkTime == 0) {
                        benchmarkTime = System.currentTimeMillis() - start;
                    } else {
                        benchmarkTime = (benchmarks * benchmarkTime + (System.currentTimeMillis() - start)) / (benchmarks + 1);
                    }

                    benchmarks++;

                    System.out.println("Filter: " + benchmarkTime);
                }
                else {
                    this.topNClusters.put(key, value);
                }
            }

            @Override
            public void close() { }
        };
    }
}
