package KELOS.Serdes;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class TripleSerializer implements Serializer<Triple<Integer, ArrayList<Double>, Long>> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String topic, Triple<Integer, ArrayList<Double>, Long> pair) {

        if (pair == null){
            return null;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        ArrayListSerializer serializer = new ArrayListSerializer();

        try {
            out.writeInt(pair.getLeft());
            out.writeLong(pair.getRight());
            baos.write(serializer.serialize("", pair.getMiddle()));

        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    @Override
    public void close() {

    }
}
