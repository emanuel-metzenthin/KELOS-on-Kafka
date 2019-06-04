package KELOS;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import KELOS.Serdes.ClusterDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

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
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ClusterDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<Integer, Cluster> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(ClusterProcessorService.TOPIC));

        while (true){
            ConsumerRecords<Integer, Cluster> records = consumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<Integer, Cluster> record : records) {
                System.out.print("KELOS.Cluster: " + record.key() + ", Size = " + record.value().size + " Centroid at [");

                for (double value : record.value().centroid){
                    System.out.print("" + value + ", ");
                }

                System.out.println("]");
            }
        }
    }
}