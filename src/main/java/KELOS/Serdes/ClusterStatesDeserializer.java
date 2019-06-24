package KELOS.Serdes;

import KELOS.Cluster;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class ClusterStatesDeserializer implements Deserializer<ArrayList<Cluster>> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public ArrayList<Cluster> deserialize(String topic, byte[] bytes) {

        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(bais);
        ClusterDeserializer deserializer = new ClusterDeserializer();

        try{
            int dimensions = in.readInt();
            int k = in.readInt();
            bais.reset();
            int clusterSize = 4 + 4 + 4 + 4 + 3 * 8 + 5 * 8 * dimensions + 4 * k; // Size of one cluster object in bytes
            int clusterCount = bais.available() / clusterSize;

            ArrayList<Cluster> clusters = new ArrayList<>();

            for (int i = 0; i < clusterCount; i++) {
                byte[] b = new byte[clusterSize];
                bais.read(b, 0, clusterSize);

                Cluster c = deserializer.deserialize("", b);
                clusters.add(c);
            }


            return clusters;
        } catch (IOException e){
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public void close() {

    }
}