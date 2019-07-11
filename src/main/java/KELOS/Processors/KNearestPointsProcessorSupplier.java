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

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                this.pointClusters = (KeyValueStore<Integer, Cluster>) context.getStateStore("PointBuffer");
                this.candidatePoints = (KeyValueStore<Integer, Cluster>) context.getStateStore("CandidatePoints");

                this.context.schedule(WINDOW_TIME, PunctuationType.STREAM_TIME, timestamp -> {
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

                    for (KeyValueIterator<Integer, Cluster> it = this.candidatePoints.all(); it.hasNext();){
                        KeyValue<Integer, Cluster> kv = it.next();
                        Cluster cluster = kv.value;

                        cluster.calculateKNearestNeighbors(this.pointClusters.all());

                        Pair<Cluster, Boolean> pair = Pair.of(cluster, true);
                        context.forward(kv.key, pair);
                        System.out.println("KNN Points forward: " + kv.key);
                        context.commit();

                        this.candidatePoints.delete(kv.key);
                    }

                    for (KeyValueIterator<Integer, Cluster> it = this.pointClusters.all(); it.hasNext();){
                        KeyValue<Integer, Cluster> kv = it.next();
                        Pair<Cluster, Boolean> pair = Pair.of(kv.value, false);
                        context.forward(kv.key, pair);

                        this.pointClusters.delete(kv.key);
                    }
                });
            }

            @Override
            public void process(Integer key, Pair<Cluster, Boolean> cluster) {
                if(cluster.getRight()) {
                    this.candidatePoints.put(key, cluster.getLeft());
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