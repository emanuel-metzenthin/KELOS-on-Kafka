package KELOS;

import KELOS.Serdes.ArrayListDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

import static KELOS.Main.CLUSTER_ASSIGNMENT_TOPIC;
import static KELOS.Main.DENSITIES_TOPIC;

public class AssignmentConsumer {

    static String GROUP_ID = "assignment-consumer";
    static String SERVER_CONFIGS = "localhost:9092";


    public static void main(String[] args) {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AssignmentConsumer.SERVER_CONFIGS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, AssignmentConsumer.GROUP_ID);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ArrayListDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<Integer, ArrayList<Double>> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(CLUSTER_ASSIGNMENT_TOPIC));

        while (true){
            ConsumerRecords<Integer, ArrayList<Double>> records = consumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<Integer, ArrayList<Double>> record : records) {
                System.out.print("\n KELOS.Cluster: " + record.key() + ", [");

                for (double d : record.value()){
                    System.out.print("" + d+ ", ");
                }
                System.out.print("]");
            }
        }
    }
}