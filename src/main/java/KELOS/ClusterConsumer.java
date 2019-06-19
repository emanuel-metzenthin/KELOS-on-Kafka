package KELOS;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import KELOS.Serdes.ClusterDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import static KELOS.Main.CLUSTER_TOPIC;
import static KELOS.Main.DENSITIES_TOPIC;

public class ClusterConsumer {

    static String GROUP_ID = "debug-consumer";
    static String SERVER_CONFIGS = "localhost:9092";


    public static void main(String[] args) {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ClusterConsumer.SERVER_CONFIGS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, ClusterConsumer.GROUP_ID);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DoubleDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<Integer, Double> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(DENSITIES_TOPIC));

        while (true){
            ConsumerRecords<Integer, Double> records = consumer.poll(Duration.ofSeconds(1));
            int count = 0;

            for (ConsumerRecord<Integer, Double> record : records) {
                // System.out.print("KELOS.Cluster: " + record.key() + ", Size = " + record.value().size + " Centroid at [");
                System.out.print("\n KELOS.Cluster: " + record.key() + ", Density = " + record.value());
//
                count ++;
//                for (double value : record.value().centroid){
//                    System.out.print("" + value + ", ");
//                }
//
//                System.out.println("]");
            }

            System.out.print("\n Cluster count: " + count);
        }
    }
}