package KELOS.Serdes;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class TripleDeserializer implements Deserializer<Triple<Integer, ArrayList<Double>, Long>> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public Triple<Integer, ArrayList<Double>, Long> deserialize(String topic, byte[] bytes) {

        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(bais);
        ArrayList<Double> list = new ArrayList<>();

        try{
            int index = in.readInt();

            long timestamp = in.readLong();

            while (in.available() > 0) {
                double element = in.readDouble();
                list.add(element);
            }

            return Triple.of(index, list, timestamp);
        } catch (IOException e){
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public void close() {

    }
}