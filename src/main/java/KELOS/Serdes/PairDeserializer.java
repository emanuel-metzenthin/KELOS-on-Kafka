package KELOS.Serdes;

import KELOS.Cluster;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.Deserializer;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;

public class PairDeserializer implements Deserializer<Pair<Cluster, Boolean>> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public Pair<Cluster, Boolean> deserialize(String topic, byte[] bytes) {

        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(bais);
        ClusterDeserializer deserializer = new ClusterDeserializer();

        try{
            int dimensions = in.readInt();
            int k = in.readInt();
            bais.reset();
            int clusterSize = 4 + 4 + 4 + 4 + 3 * 8 + 5 * 8 * dimensions + 4 * k; // Size of one cluster object in bytes
            byte[] b = new byte[clusterSize];
            bais.read(b, 0, clusterSize);

            Cluster c = deserializer.deserialize("", b);

            return Pair.of(c, in.readBoolean());
        } catch (IOException e){
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public void close() {

    }
}