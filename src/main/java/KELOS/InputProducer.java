package KELOS;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Properties;

import KELOS.Serdes.ArrayListSerializer;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class InputProducer {

//    static String CSV_DATA = "./cluster_test_data.csv";
    //static String CSV_DATA = "./evaluation_data_unlabeled.csv";
    static String CSV_DATA = "./test_data_unlabeled.csv";
    static String TOPIC = "data-input";
    static String APP_ID = "input-producer";
    static String SERVER_CONFIGS = "localhost:9092";

    public static void main(String[] args) {
        runProducer(500);
    }

    static void runProducer(int elementsPerWindow) {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, InputProducer.SERVER_CONFIGS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, InputProducer.APP_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ArrayListSerializer.class.getName());

        Producer<Integer, ArrayList<Double>> producer = new KafkaProducer<>(props);

        File csvData = new File(InputProducer.CSV_DATA);

        try {
            CSVParser parser = CSVParser.parse(csvData, StandardCharsets.UTF_8, CSVFormat.RFC4180);

            Long timestamp = System.currentTimeMillis();
            double total_window_time_millis = Main.WINDOW_TIME.toMillis() * Main.AGGREGATION_WINDOWS;

            int count = 0;
            for (CSVRecord csvRecord : parser) {

//                if (count == 50000){
//                    break;
//                }

                ArrayList<Double> numberRecord = new ArrayList<>();

                for (int i = 0; i < csvRecord.size(); i++){
                    String val = csvRecord.get(i);

                    if (NumberUtils.isParsable(val)){
                        double number = Double.valueOf(val);
                        numberRecord.add(number);
                    } else {
                        System.out.println("Not parsable " + val);
                    }
                }

                Thread.sleep(100);

                timestamp += (int) (total_window_time_millis) / elementsPerWindow;

                producer.send(new ProducerRecord<>(InputProducer.TOPIC, 0, timestamp, count, numberRecord));
                count++;
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

    }
}