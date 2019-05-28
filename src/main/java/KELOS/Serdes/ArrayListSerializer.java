package KELOS.Serdes;

import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class ArrayListSerializer implements Serializer<ArrayList<Double>> {
    @Override
    public void configure(Map<String, ?> config, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, ArrayList<Double> arrayList) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        for (double element : arrayList) {
            try {
                out.writeDouble(element);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return baos.toByteArray();
    }

    @Override
    public void close() {

    }
}
