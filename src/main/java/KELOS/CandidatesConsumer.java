package KELOS;

import KELOS.Serdes.PairDeserializer;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.tuple.Pair;
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

import static KELOS.Main.CANDIDATES_TOPIC;

public class CandidatesConsumer {

    static String GROUP_ID = "debug-consumer";
    static String SERVER_CONFIGS = "localhost:9092";


    public static void main(String[] args) {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CandidatesConsumer.SERVER_CONFIGS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, CandidatesConsumer.GROUP_ID);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, PairDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<Integer, Pair<Cluster, Boolean>> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(CANDIDATES_TOPIC));

        try {
            File file = new File("./candidates.csv");

            file.createNewFile();

            if(file.delete()) {
                file.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        while (true){
            try {
                BufferedWriter writer = Files.newBufferedWriter(Paths.get("./candidates.csv"), StandardCharsets.UTF_8, StandardOpenOption.APPEND);

                ConsumerRecords<Integer, Pair<Cluster, Boolean>> records = consumer.poll(Duration.ofSeconds(1));

                CSVPrinter csvPrinter = new CSVPrinter(writer, CSVFormat.DEFAULT);

                for (ConsumerRecord<Integer,Pair<Cluster, Boolean>> record : records) {
                    if(record.value().getRight() && record.key() != -1) {
                        ArrayList<String> rec = new ArrayList<>();
                        rec.add(record.key() + "");

                        for(Double d : record.value().getLeft().centroid) {
                            rec.add(d + "");
                        }

                        csvPrinter.printRecord(rec);
                    }
                }
                csvPrinter.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}