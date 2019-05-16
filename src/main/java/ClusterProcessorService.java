/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public final class ClusterProcessorService {

    static final double DISTANCE_THRESHOLD = 20;

    static class ClusterRegistrationProcessorSupplier implements ProcessorSupplier<String, ArrayList<Double>> {

        @Override
        public Processor<String, ArrayList<Double>> get() {
            return new Processor<String, ArrayList<Double>>() {
                private KeyValueStore<Integer, Cluster> state;

                @Override
                public void init(ProcessorContext context) {
                    this.state = (KeyValueStore<Integer, Cluster>) context.getStateStore("Clusters");
                }

                @Override
                public void process(String key, ArrayList<Double> value) {
                    // can access this.state
                    // can emit as many new KeyValue pairs as required via this.context#forward()

                    KeyValueIterator<Integer, Cluster> clusters = this.state.all();


                    if(!clusters.hasNext()){
                        Cluster dummy = new Cluster();
                        dummy.size = 1;
                        dummy.linearSums = new double[]{3, 4};
                        dummy.minimums = new double[]{3, 4};
                        dummy.maximums = new double[]{3, 4};

                        this.state.put(3, dummy);
                    }

                    clusters.close();
                }

                @Override
                public void close() {
                    // can access this.state
                    // can emit as many new KeyValue pairs as required via this.context#forward()
                }
            };
        }
    }

    static class ClusteringProcessorSupplier implements ProcessorSupplier<String, ArrayList<Double>> {

        @Override
        public Processor<String, ArrayList<Double>> get() {
            return new Processor<String, ArrayList<Double>>() {
                private ProcessorContext context;
                private KeyValueStore<Integer, Cluster> tempClusters;

                @Override
                public void init(ProcessorContext context) {
                    this.context = context;
                    this.tempClusters = (KeyValueStore<Integer, Cluster>) context.getStateStore("TempClusters");
                    KeyValueStore<Integer, Cluster> clusters = (KeyValueStore<Integer, Cluster>) context.getStateStore("Clusters");

                    // Clear all meta data in cluster store, but keep centroids for distance computation
                    for(KeyValueIterator<Integer, Cluster> i = clusters.all(); i.hasNext();) {
                        KeyValue<Integer, Cluster> cluster = i.next();
                        Cluster emptyCluster = new Cluster(cluster.value.centroid.length);
                        emptyCluster.centroid = cluster.value.centroid;
                        this.tempClusters.put(cluster.key, emptyCluster);
                    }

                    // Emit cluster meta data after sub-window has been processed
                    this.context.schedule(1000, PunctuationType.STREAM_TIME, (timestamp) -> {
                        for(KeyValueIterator<Integer, Cluster> i = this.tempClusters.all(); i.hasNext();) {
                            KeyValue<Integer, Cluster> cluster = i.next();
                            context.forward(cluster.key, cluster.value);
                        }

                        // commit the current processing progress
                        context.commit();
                    });
                }

                @Override
                public void process(String key, ArrayList<Double> value) {
                    double minDist = Double.MAX_VALUE;
                    Cluster cluster = null;
                    int clusterIdx = 0;
                    int numCluster = 0;

                    for(KeyValueIterator<Integer, Cluster> i = this.tempClusters.all(); i.hasNext();) {
                        KeyValue<Integer, Cluster> c = i.next();

                        double dist = c.value.distance(value);

                        if (dist < minDist) {
                            minDist = dist;
                            cluster = c.value;
                            clusterIdx = c.key;
                        }

                        numCluster++;
                    }

                    if (minDist < ClusterProcessorService.DISTANCE_THRESHOLD) {
                        cluster.addRecord(value);
                        this.tempClusters.put(clusterIdx, cluster);
                    } else {
                        this.tempClusters.put(numCluster + 1, new Cluster(value));
                    }
                }

                @Override
                public void close() {
                    // can access this.state
                    // can emit as many new KeyValue pairs as required via this.context#forward()
                }
            };
        }
    }

    static String TOPIC = "clusters";
    static String APP_ID = "cluster-service";
    static String SERVER_CONFIGS = "localhost:9092";

    public static void main(final String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, ClusterProcessorService.APP_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, ClusterProcessorService.SERVER_CONFIGS);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, ArrayListSerde.class.getName());

        // setting offset reset to earliest so that we can re-run the demo code with the same pre-loaded data
        // props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final Topology builder = new Topology();

        builder.addSource("Source", InputProducer.TOPIC);

        builder.addGlobalStore(
                Stores.keyValueStoreBuilder(
                    Stores.inMemoryKeyValueStore("Clusters"),
                    Serdes.Integer(),
                    new ClusterSerde()
                ).withLoggingDisabled(),
                "GlobalSource",
                new StringDeserializer(),
                new ArrayListDeserializer(),
                ClusterProcessorService.TOPIC,
                "ClusterRegistration",
                new ClusterRegistrationProcessorSupplier());

        builder.addStateStore(
                Stores.keyValueStoreBuilder(
                        Stores.inMemoryKeyValueStore("TempClusters"),
                        Serdes.Integer(),
                        new ClusterSerde()),
                "ClusteringProcessor");

        builder.addProcessor("ClusteringProcessor", new ClusteringProcessorSupplier(), "Source");

        builder.addSink("Sink", ClusterProcessorService.TOPIC, "Source");



        // ==== SHUTDOWN ====

        final KafkaStreams streams = new KafkaStreams(builder, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}