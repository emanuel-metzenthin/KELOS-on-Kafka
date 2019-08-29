package KELOS.Processors;

import KELOS.Cluster;
import KELOS.GaussianKernel;
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
import java.util.ArrayList;
import java.util.Date;
import java.util.TimeZone;

import static KELOS.Main.K;

public class PointDensityEstimationProcessorSupplier implements ProcessorSupplier<Integer, Pair<Cluster, Boolean>> {

    /*
        Estimates the density for the candidates as well as for the nearest neighbors of the candidates.
     */
    @Override
    public Processor<Integer, Pair<Cluster, Boolean>> get() {
        return new Processor<Integer, Pair<Cluster, Boolean>>() {
            private ProcessorContext context;
            private KeyValueStore<Integer, Pair<Cluster, Boolean>> windowPoints;

            @Override
            public void init(ProcessorContext context) {
                this.context = context;
                this.windowPoints = (KeyValueStore<Integer, Pair<Cluster, Boolean>>) context.getStateStore("PointDensityBuffer");
            }

            @Override
            public void process(Integer key, Pair<Cluster, Boolean> value) {
                if (Cluster.isEndOfWindowToken(value.getLeft())){
                    for(KeyValueIterator<Integer, Pair<Cluster, Boolean>> it = this.windowPoints.all(); it.hasNext();) {
                        KeyValue<Integer, Pair<Cluster, Boolean>> kv = it.next();

                        Integer neighborKey = kv.key;
                        Cluster point = kv.value.getLeft();

                        // Don't compute density for the clusters, we already did that earlier
                        if (!kv.value.getRight()){
                            this.context.forward(neighborKey, Pair.of(point, false));
                            continue;
                        }

                        ArrayList<Cluster> kNNs = new ArrayList<>();

                        for(int i : point.knnIds) {
                            if (this.windowPoints.get(i) != null) {
                                kNNs.add(this.windowPoints.get(i).getLeft());
                            }
                        }

                        if (kNNs.size() <= 1) {
                            continue;
                        }

                        int k = kNNs.size();
                        int d = kNNs.get(0).centroid.length;

                        double pointWeight = 1.0 / k;

                        ArrayList<Double> dimensionMeans = new ArrayList<>();

                        for(int i = 0; i < d; i++) {
                            double mean = 0;

                            for(int m = 0; m < k; m++) {
                                mean += kNNs.get(m).centroid[i] * pointWeight;
                            }

                            mean /= k;

                            dimensionMeans.add(mean);
                        }

                        ArrayList<Double> dimensionStdDevs = new ArrayList<>();

                        for(int i = 0; i < d; i++) {
                            double stdDev = 0;

                            for(int m = 0; m < k; m++) {
                                double diffToMean = kNNs.get(m).centroid[i] - dimensionMeans.get(i);
                                stdDev += Math.pow(diffToMean, 2) * pointWeight;
                            }

                            stdDev = Math.sqrt(stdDev);

                            dimensionStdDevs.add(stdDev);
                        }

                        ArrayList<Double> dimensionBandwidths = new ArrayList<>();

                        for(int i = 0; i < d; i++) {
                            double bandwidth = 1.06 * dimensionStdDevs.get(i) * Math.pow(k, -1.0 / (d + 1));
                            dimensionBandwidths.add(bandwidth);
                        }

                        point.density = 0;

                        for(int i = 0; i < k; i++) {
                            double productKernel = 1;

                            for(int j = 0; j < d; j++) {
                                double difference = Math.abs(point.centroid[j] - kNNs.get(i).centroid[j]);
                                productKernel *= new GaussianKernel(dimensionBandwidths.get(j)).computeDensity(difference);
                            }

                            point.density += productKernel * pointWeight;
                        }

                        // Forward point with candidate indication
                        this.context.forward(neighborKey, Pair.of(point, true));
                    }

                    // Forward EndOfWindowToken
                    this.context.forward(key, Pair.of(value.getLeft(), false));

                    for(KeyValueIterator<Integer, Pair<Cluster, Boolean>> i = this.windowPoints.all(); i.hasNext();) {
                        KeyValue<Integer, Pair<Cluster, Boolean>> cluster = i.next();

                        this.windowPoints.delete(cluster.key);
                    }
                }
                else {
                    this.windowPoints.put(key, value);
                }

            }

            @Override
            public void close() { }
        };
    }
}