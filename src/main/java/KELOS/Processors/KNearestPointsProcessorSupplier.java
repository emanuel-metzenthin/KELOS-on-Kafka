package KELOS.Processors;

import KELOS.Cluster;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;


public class KNearestPointsProcessorSupplier implements ProcessorSupplier<Integer, Pair<Cluster, Boolean>> {

    /*
        Finds the K nearest neighbors for each candidate point, as well as for the K nearest neighbors of the candidates.
        The latter is necessary because we also need to compute the density for the neighbors to calculate the relative density
        (KLOME-Score) for a candidate.
    */
    @Override
    public Processor<Integer, Pair<Cluster, Boolean>> get() {
        return new Processor<Integer, Pair<Cluster, Boolean>>() {
            private ProcessorContext context;
            private KeyValueStore<Integer, Cluster> clusters;
            private KeyValueStore<Integer, Cluster> candidatePoints;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                this.clusters = (KeyValueStore<Integer, Cluster>) context.getStateStore("PointBuffer");
                this.candidatePoints = (KeyValueStore<Integer, Cluster>) context.getStateStore("CandidatePoints");
            }

            @Override
            public void process(Integer key, Pair<Cluster, Boolean> value) {
                if (Cluster.isEndOfWindowToken(value.getLeft())){

                    // Compute KNNs for candidate points
                    for (KeyValueIterator<Integer, Cluster> it = this.candidatePoints.all(); it.hasNext();) {
                        KeyValue<Integer, Cluster> kv = it.next();

                        Cluster cluster = kv.value;

                        cluster.calculateKNearestNeighbors(this.clusters.all(), kv.key);

                        // Forward indicating point is a candidate
                        Pair<Cluster, Boolean> pair = Pair.of(cluster, true);
                        context.forward(kv.key, pair);

                        this.candidatePoints.delete(kv.key);
                    }

                    for (KeyValueIterator<Integer, Cluster> it = this.clusters.all(); it.hasNext();){
                        KeyValue<Integer, Cluster> kv = it.next();

                        // Forward indicating point is a candidate
                        Pair<Cluster, Boolean> pair = Pair.of(kv.value, false);
                        context.forward(kv.key, pair);

                        this.clusters.delete(kv.key);
                    }

                    context.forward(key, Pair.of(value.getLeft(), false));
                }
                else {
                    if(value.getRight()) {
                        this.candidatePoints.put(key, value.getLeft());
                    } else {
                        this.clusters.put(key, value.getLeft());
                    }
                }
            }

            @Override
            public void close() {

            }
        };
    }
}