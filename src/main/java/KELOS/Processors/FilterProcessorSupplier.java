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


public class FilterProcessorSupplier implements ProcessorSupplier<Integer, Pair<Cluster, Boolean>> {
    /*
        Filters all points in window based on whether they belong to the clusters that could contain outliers.
     */
    @Override
    public Processor<Integer, Pair<Cluster, Boolean>> get() {
        return new Processor<Integer, Pair<Cluster, Boolean>>() {
            private ProcessorContext context;
            private KeyValueStore<Integer, Pair<Cluster, Boolean>> clusters;
            private WindowStore<Integer, Triple<Integer, ArrayList<Double>, Long>> windowPoints;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                this.clusters = (KeyValueStore<Integer, Pair<Cluster, Boolean>>) context.getStateStore("ClustersWithCandidates");
                this.windowPoints = (WindowStore<Integer, Triple<Integer, ArrayList<Double>, Long>>) context.getStateStore("ClusterAssignments");
            }

            @Override
            public void process(Integer key, Pair<Cluster, Boolean> value) {
                if (Cluster.isEndOfWindowToken(value.getLeft())){
                    // Fetch all points and their cluster assignment in current aggregated window
                    long fromTime = this.context.timestamp() - (long) (((double) AGGREGATION_WINDOWS - 0.5) * WINDOW_TIME.toMillis());
                    long toTime = this.context.timestamp();

                    for(KeyValueIterator<Windowed<Integer>, Triple<Integer, ArrayList<Double>, Long>> i = this.windowPoints.fetchAll(fromTime, toTime); i.hasNext();) {
                        KeyValue<Windowed<Integer>, Triple<Integer, ArrayList<Double>, Long>> point = i.next();

                        // Check if the point's cluster is one of the clusters that could contain outliers
                        Pair<Cluster, Boolean> cluster = this.clusters.get(point.value.getLeft());

                        // Create cluster representation of the point
                        Cluster singlePointCluster = new Cluster(point.value.getMiddle(), K);

                        if (cluster != null && cluster.getRight()){
                            // Indicate the point is an outlier candidate
                            Pair<Cluster, Boolean> pair = Pair.of(singlePointCluster, true);
                            this.context.forward(point.key.key(), pair);
                        }
                    }

                    for(KeyValueIterator<Integer, Pair<Cluster, Boolean>> i = this.clusters.all(); i.hasNext();) {
                        KeyValue<Integer, Pair<Cluster, Boolean>> cluster = i.next();

                        // Forward the clusters and indicate that these are non-candidates
                        Pair<Cluster, Boolean> pair = Pair.of(cluster.value.getLeft(), false);
                        this.context.forward(cluster.key, pair);

                        this.clusters.delete(cluster.key);
                    }

                    // Forward EndOfWindowToken
                    this.context.forward(key, Pair.of(value.getLeft(), true));
                }
                else {
                    this.clusters.put(key, value);
                }
            }

            @Override
            public void close() { }
        };
    }
}
