package KELOS;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
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

    // static String CSV_DATA = "./data/kddcup.data_10_percent_corrected";
    static String CSV_DATA = "./cluster_test_data.csv";
    static String TOPIC = "data-input";
    static String APP_ID = "input-producer";
    static String SERVER_CONFIGS = "localhost:9092";

    public static void main(String[] args) {
        runProducer();
    }

    static void runProducer() {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, InputProducer.SERVER_CONFIGS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, InputProducer.APP_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ArrayListSerializer.class.getName());

        Producer<Integer, ArrayList<Double>> producer = new KafkaProducer<>(props);

        File csvData = new File(InputProducer.CSV_DATA);

        try {
            CSVParser parser = CSVParser.parse(csvData, Charset.forName("UTF-8"), CSVFormat.RFC4180);

            Long timestamp = System.currentTimeMillis();

            for (CSVRecord csvRecord : parser) {
                ArrayList<Double> numberRecord = new ArrayList<>();

                for (int i = 0; i < csvRecord.size(); i++){
                    String val = csvRecord.get(i);

                    if (NumberUtils.isParsable(val)){
                        double number = Double.parseDouble(val);
                        numberRecord.add(number);
                    }
                }

                timestamp += 100;

                producer.send(new ProducerRecord<>(InputProducer.TOPIC, 0, timestamp, 0, numberRecord));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}