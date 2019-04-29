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

    public static void main(String[] args) {

        runProducer();
    }

    static void runProducer() {
        Properties props = new Properties();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "InputProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);

        File csvData = new File("./data/kddcup.data_10_percent_corrected");
        try {
            CSVParser parser = CSVParser.parse(csvData, Charset.forName("UTF-8"), CSVFormat.RFC4180);

            for (CSVRecord csvRecord : parser) {
                ArrayList<Double> numberRecord = new ArrayList<>();
                String stringRecord = "";

                for (int i = 0; i < csvRecord.size(); i++){
                    String val = csvRecord.get(i);

                    if (NumberUtils.isParsable(val)){
                        stringRecord += "," + val;
                        double number = Double.parseDouble(val);
                        numberRecord.add(number);
                    }
                }

                producer.send(new ProducerRecord<>("data-input", stringRecord));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}