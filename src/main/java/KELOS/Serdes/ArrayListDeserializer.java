package KELOS.Serdes;

import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class ArrayListDeserializer implements Deserializer<ArrayList<Double>> {
    @Override
    public void configure(Map<String, ?> config, boolean isKey) {

    }

    @Override
    public ArrayList<Double> deserialize(String topic, byte[] bytes) {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(bais);
        ArrayList<Double> list = new ArrayList<>();

        try{
            while (in.available() > 0) {
                double element = in.readDouble();
                list.add(element);
            }
        } catch (IOException e){
            e.printStackTrace();
        }

        return list;
    }

    @Override
    public void close() {

    }
}
