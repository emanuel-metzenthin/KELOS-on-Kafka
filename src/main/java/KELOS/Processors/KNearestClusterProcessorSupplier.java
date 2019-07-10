package KELOS.Processors;

import KELOS.Cluster;
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
    Finds the K nearest neighbors for each input Cluster.
 */
public class KNearestClusterProcessorSupplier implements ProcessorSupplier<Integer, Cluster> {

    @Override
    public Processor<Integer, Cluster> get() {
        return new Processor<Integer, Cluster>() {
            private ProcessorContext context;
            private KeyValueStore<Integer, Cluster> clusters;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                this.clusters = (KeyValueStore<Integer, Cluster>) context.getStateStore("ClusterBuffer");

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

                    System.out.println("New KNN window: " + dateFormatted + " System time : " + systime);

                    for (KeyValueIterator<Integer, Cluster> it = this.clusters.all(); it.hasNext(); ) {
                        KeyValue<Integer, Cluster> kv = it.next();
                        Cluster cluster = kv.value;

                        cluster.calculateKNearestNeighbors(this.clusters.all());

                        System.out.println("KNN forward: " + kv.key);

                        context.forward(kv.key, cluster);
                        context.commit();
                    }

                    for (KeyValueIterator<Integer, Cluster> it = this.clusters.all(); it.hasNext(); ) {
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
        };
    }
}