package KELOS.Serdes;

import KELOS.Cluster;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class PairDeserializer implements Deserializer<Pair<Cluster, Integer>> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public Pair<Cluster, Integer> deserialize(String topic, byte[] bytes) {

        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(bais);
        ClusterDeserializer d = new ClusterDeserializer();
        Cluster c;

        try{
            byte[] b = new byte[bytes.length - 4];
            bais.read(b, 0, bytes.length - 4);
            c = d.deserialize("", bytes);

            int flag = in.readInt();

            return Pair.of(c, flag);
        } catch (IOException e){
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public void close() {

    }
}