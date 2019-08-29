package KELOS;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Properties;

import KELOS.Serdes.ArrayListSerializer;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.IntegerSerializer;

public class InputProducer {

    static String CSV_DATA = "./gmm_test_data_unlabeled.csv";
    //static String CSV_DATA = "./test_data_unlabeled.csv";
    static String TOPIC = "data-input";
    static String APP_ID = "input-producer";
    static String SERVER_CONFIGS = "localhost:9092";

    static int ELEMENTS_PER_WINDOW = 3000;

    public static void main(String[] args) {
        runProducer(ELEMENTS_PER_WINDOW);
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

            long timestamp = System.currentTimeMillis();

            int count = 0;
            for (CSVRecord csvRecord : parser) {
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

                if (count % (elementsPerWindow / Main.AGGREGATION_WINDOWS) == 0){
                    timestamp += Main.WINDOW_TIME.toMillis();
                }

                producer.send(new ProducerRecord<>(InputProducer.TOPIC, 0, timestamp, count, numberRecord));
                count++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}