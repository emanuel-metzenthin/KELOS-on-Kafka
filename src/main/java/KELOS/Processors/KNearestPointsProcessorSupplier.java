package KELOS.Processors;

import KELOS.Cluster;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashSet;
import java.util.TimeZone;

import static KELOS.Main.WINDOW_TIME;

/*
    Finds the K nearest neighbors for each input point.
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

                this.context.schedule(WINDOW_TIME, PunctuationType.STREAM_TIME, timestamp -> {
                    long start = System.currentTimeMillis();
                /*
                HashMap<Integer, Cluster> uniqueClusters = new HashMap<>();
                for(KeyValueIterator<Windowed<Integer>, Cluster> i = this.clusters.all(); i.hasNext();) {
                    KeyValue<Windowed<Integer>, Cluster> cluster = i.next();
                    uniqueClusters.put(cluster.key.key(), cluster.value);
                }
                */
                    Date date = new Date(timestamp);
                    DateFormat formatter = new SimpleDateFormat("HH:mm:ss.SSS");
                    formatter.setTimeZone(TimeZone.getTimeZone("Europe/Berlin"));
                    String dateFormatted = formatter.format(date);
                    String systime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS"));

                    System.out.println("New KNN Points window: " + dateFormatted + " System time : " + systime);
                    HashSet<Integer> candidates = new HashSet<>();
                    HashSet<Integer> candidateKNNs = new HashSet<>();
                    HashSet<Integer> knnKNNs = new HashSet<>();

                    for (KeyValueIterator<Integer, Cluster> it = this.candidatePoints.all(); it.hasNext();) {
                        KeyValue<Integer, Cluster> kv = it.next();
                        Cluster cluster = kv.value;

                        cluster.calculateKNearestNeighbors(this.pointClusters.all(), kv.key);

                        candidates.add(kv.key);
                        this.pointClusters.put(kv.key, cluster);

                        Pair<Cluster, Integer> pair = Pair.of(cluster, 0);
                        context.forward(kv.key, pair);
                        System.out.println("KNN Points candidate forward: " + kv.key);

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
                        System.out.println("KNN Points KNN forward: " + index);
                    }

                    for (int index : knnKNNs){
                        Cluster cluster = this.pointClusters.get(index);

                        Pair<Cluster, Integer> pair = Pair.of(cluster, 2);
                        context.forward(index, pair);
                        System.out.println("KNN Points neighbor KNN forward: " + index);
                    }

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
                });
            }

            @Override
            public void process(Integer key, Pair<Cluster, Boolean> cluster) {
                if(cluster.getRight()) {
                    this.candidatePoints.put(key, cluster.getLeft());
                    this.pointClusters.put(key, cluster.getLeft());
                } else {
                    this.pointClusters.put(key, cluster.getLeft());
                }
            }

            @Override
            public void close() {

            }
        };
    }
}