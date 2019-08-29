package KELOS.Serdes;
import KELOS.Cluster;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

public class PairSerializer implements Serializer<Pair<Cluster, Boolean>> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String topic, Pair<Cluster, Boolean> pair) {

        if (pair == null){
            return null;
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        ClusterSerializer serializer = new ClusterSerializer();

        try {
            baos.write(serializer.serialize("", pair.getLeft()));
            out.writeBoolean(pair.getRight());

        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    @Override
    public void close() {

    }
}
