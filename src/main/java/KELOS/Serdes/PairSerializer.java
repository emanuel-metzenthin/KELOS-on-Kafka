package KELOS.Serdes;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class PairSerializer implements Serializer<Pair<Integer, ArrayList<Double>>> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String topic, Pair<Integer, ArrayList<Double>> pair) {

        if (pair == null){
            return null;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        ArrayListSerializer serializer = new ArrayListSerializer();

        try {
            out.writeInt(pair.getLeft());
            baos.write(serializer.serialize("", pair.getRight()));

        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    @Override
    public void close() {

    }
}
