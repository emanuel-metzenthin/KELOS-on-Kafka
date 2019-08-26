package KELOS.Processors;

import KELOS.Cluster;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashSet;
import java.util.TimeZone;

/*
    Finds the K nearest neighbors for each candidate point, as well as for the K nearest neighbors of the candidates.
    The latter is necessary because we also need to compute the density for the neighbors to calculate the relative density
    (KLOME-Score) for a candidate.
 */
public class KNearestPointsProcessorSupplier implements ProcessorSupplier<Integer, Pair<Cluster, Boolean>> {

    @Override
    public Processor<Integer, Pair<Cluster, Boolean>> get() {
        return new Processor<Integer, Pair<Cluster, Boolean>>() {
            private ProcessorContext context;
            private KeyValueStore<Integer, Cluster> pointClusters;
            private KeyValueStore<Integer, Cluster> candidatePoints;
            private long benchmarkTime = 0;
            private int benchmarks = 0;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                this.pointClusters = (KeyValueStore<Integer, Cluster>) context.getStateStore("PointBuffer");
                this.candidatePoints = (KeyValueStore<Integer, Cluster>) context.getStateStore("CandidatePoints");
            }

            @Override
            public void process(Integer key, Pair<Cluster, Boolean> value) {

                if (Cluster.isEndOfWindowToken(value.getLeft())){
                    long start = System.currentTimeMillis();

                    Date date = new Date(this.context.timestamp());
                    DateFormat formatter = new SimpleDateFormat("HH:mm:ss.SSS");
                    formatter.setTimeZone(TimeZone.getTimeZone("Europe/Berlin"));
                    String dateFormatted = formatter.format(date);
                    String systime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS"));

                    System.out.println("New KNN Points window: " + dateFormatted + " System time : " + systime);

                    HashSet<Integer> candidates = new HashSet<>();
                    HashSet<Integer> candidateKNNs = new HashSet<>();
                    HashSet<Integer> knnKNNs = new HashSet<>();
                    boolean first = true;

                    for (KeyValueIterator<Integer, Cluster> it = this.candidatePoints.all(); it.hasNext();) {
                        KeyValue<Integer, Cluster> kv = it.next();

                        if(first) {
                            System.out.println("KNN points from " + kv.key);
                            first = false;
                        }
                        if(!it.hasNext()) {
                            System.out.println("KNN points last " + kv.key);
                        }
                        Cluster cluster = kv.value;

                        cluster.calculateKNearestNeighbors(this.pointClusters.all(), kv.key);

                        candidates.add(kv.key);
                        this.pointClusters.put(kv.key, cluster);

                        Pair<Cluster, Integer> pair = Pair.of(cluster, 0);
                        context.forward(kv.key, pair);

                        this.candidatePoints.delete(kv.key);
                    }

                    for (int candidate : candidates){
                        Cluster cluster = this.pointClusters.get(candidate);

                        for (int i : cluster.knnIds){
                            if (candidates.contains(i)){
                                continue;
                            }

                            candidateKNNs.add(i);

                            // Make sure each point is in at most one set
                            knnKNNs.remove(i);

                            Cluster c = this.pointClusters.get(i);
                            c.calculateKNearestNeighbors(this.pointClusters.all(), i);
                            this.pointClusters.put(i, c);

                            for (int n : c.knnIds) {
                                if (candidates.contains(n) || candidateKNNs.contains(n)){
                                    continue;
                                }
                                knnKNNs.add(n);

                                Cluster neighborsNeighbor = this.pointClusters.get(n);
                                neighborsNeighbor.calculateKNearestNeighbors(this.pointClusters.all(), n);
                                this.pointClusters.put(n, neighborsNeighbor);
                            }
                        }
                    }

                    for (int index : candidateKNNs){
                        Cluster cluster = this.pointClusters.get(index);

                        Pair<Cluster, Integer> pair = Pair.of(cluster, 1);
                        context.forward(index, pair);
                    }

                    for (int index : knnKNNs){
                        Cluster cluster = this.pointClusters.get(index);

                        Pair<Cluster, Integer> pair = Pair.of(cluster, 2);
                        context.forward(index, pair);
                    }

                    context.forward(key, Pair.of(value.getLeft(), 0));

                    for (KeyValueIterator<Integer, Cluster> it = this.pointClusters.all(); it.hasNext();){
                        KeyValue<Integer, Cluster> kv = it.next();

                        this.pointClusters.delete(kv.key);
                    }

                    if(benchmarkTime == 0) {
                        benchmarkTime = System.currentTimeMillis() - start;
                    } else {
                        benchmarkTime = (benchmarks * benchmarkTime + (System.currentTimeMillis() - start)) / (benchmarks + 1);
                    }

                    benchmarks++;

                    System.out.println("KNN Point: " + benchmarkTime);
                }
                else {
                    if(value.getRight()) {
                        this.candidatePoints.put(key, value.getLeft());
                        this.pointClusters.put(key, value.getLeft());
                    } else {
                        this.pointClusters.put(key, value.getLeft());
                    }
                }
            }

            @Override
            public void close() {

            }
        };
    }
}