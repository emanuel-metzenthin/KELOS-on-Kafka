package KELOS.Serdes;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class PairDeserializer implements Deserializer<Pair<Integer, ArrayList<Double>>> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public Pair<Integer, ArrayList<Double>> deserialize(String topic, byte[] bytes) {

        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(bais);
        ArrayList<Double> list = new ArrayList<>();

        try{
            int index = in.readInt();

            while (in.available() > 0) {
                double element = in.readDouble();
                list.add(element);
            }

            return Pair.of(index, list);
        } catch (IOException e){
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public void close() {

    }
}