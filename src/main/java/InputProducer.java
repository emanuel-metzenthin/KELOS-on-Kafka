import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

public class InputProducer {

    static String CSV_DATA = "./data/kddcup.data_10_percent_corrected";
    static String TOPIC = "data-input";
    static String APP_ID = "input-producer";
    static String SERVER_CONFIGS = "localhost:9092";

    public static void main(String[] args) {
        runProducer();
    }

    static void runProducer() {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.SERVER_CONFIGS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, this.APP_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);

        File csvData = new File(this.CSV_DATA);

        try {
            CSVParser parser = CSVParser.parse(csvData, Charset.forName("UTF-8"), CSVFormat.RFC4180);

            for (CSVRecord csvRecord : parser) {
                // TODO Comment out once Serde for ArrayList is implemented
                // ArrayList<Double> numberRecord = new ArrayList<>();
                String stringRecord = "";

                for (int i = 0; i < csvRecord.size(); i++){
                    String val = csvRecord.get(i);

                    if (NumberUtils.isParsable(val)){
                        stringRecord += "," + val;
                        // double number = Double.parseDouble(val);
                        // numberRecord.add(number);
                    }
                }

                producer.send(new ProducerRecord<>(this.TOPIC, stringRecord));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}