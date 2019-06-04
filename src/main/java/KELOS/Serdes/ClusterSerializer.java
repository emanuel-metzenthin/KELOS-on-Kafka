package KELOS.Serdes;

import KELOS.Cluster;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

public class ClusterSerializer implements Serializer<Cluster> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String topic, Cluster cluster) {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        try {
            out.writeInt(cluster.linearSums.length);
            out.writeInt(cluster.knnIds.length);
            out.writeInt(cluster.size);

            for (double element : cluster.centroid) {
                out.writeDouble(element);
            }
            for (double element : cluster.linearSums) {
                out.writeDouble(element);
            }
            for (double element : cluster.minimums) {
                out.writeDouble(element);
            }
            for (double element : cluster.maximums) {
                out.writeDouble(element);
            }
            for (int element : cluster.knnIds) {
                out.writeInt(element);
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
