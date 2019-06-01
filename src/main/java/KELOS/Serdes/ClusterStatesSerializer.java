package KELOS.Serdes;

import KELOS.Cluster;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class ClusterStatesSerializer implements Serializer<ArrayList<Cluster>> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String topic, ArrayList<Cluster> clusters) {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ClusterSerializer serializer = new ClusterSerializer();

        try {
            for (Cluster c : clusters){
                baos.write(serializer.serialize("", c));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    @Override
    public void close() {

    }
}
