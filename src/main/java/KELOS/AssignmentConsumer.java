package KELOS;

import KELOS.Serdes.TripleDeserializer;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

import static KELOS.Main.CLUSTER_ASSIGNMENT_TOPIC;

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
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, TripleDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<Integer, Triple<Integer, ArrayList<Double>, Double>> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(CLUSTER_ASSIGNMENT_TOPIC));

        try {
            File file = new File("./assignments.csv");

            file.createNewFile();

            if(file.delete()) {
                file.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        while (true){
            try {
                BufferedWriter writer = Files.newBufferedWriter(Paths.get("./assignments.csv"), StandardCharsets.UTF_8, StandardOpenOption.APPEND);

                ConsumerRecords<Integer, Triple<Integer, ArrayList<Double>, Double>> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<Integer, Triple<Integer, ArrayList<Double>, Double>> record : records) {
                    String line = record.key() + ", " + record.value().getLeft();
                    for(double d : record.value().getMiddle()) {
                        line += "," + d;
                    }
                    writer.append(line + "\n");
                }
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}