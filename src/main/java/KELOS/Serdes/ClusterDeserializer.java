package KELOS.Serdes;

import KELOS.Cluster;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Map;

public class ClusterDeserializer implements Deserializer<Cluster> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public Cluster deserialize(String topic, byte[] bytes) {

        if (bytes == null){
            return null;
        }

        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(bais);

        try{
            int dimensions = in.readInt();
            int k = in.readInt();
            Cluster c = new Cluster(dimensions, k);
            c.size = in.readInt();
            c.oldSize = in.readInt();
            c.density = in.readDouble();
            c.minDensityBound = in.readDouble();
            c.maxDensityBound = in.readDouble();


            for(int i = 0; i < dimensions; i++){
                double element = in.readDouble();
                c.centroid[i] = element;
            }
            for(int i = 0; i < dimensions; i++){
                double element = in.readDouble();
                c.linearSums[i] = element;
            }
            for(int i = 0; i < dimensions; i++){
                double element = in.readDouble();
                c.oldLinearSums[i] = element;
            }
            for(int i = 0; i < dimensions; i++){
                double element = in.readDouble();
                c.minimums[i] = element;
            }
            for(int i = 0; i < dimensions; i++){
                double element = in.readDouble();
                c.maximums[i] = element;
            }
            for(int i = 0; i < k; i++){
                int element = in.readInt();
                c.knnIds[i] = element;
            }

            return c;
        } catch (IOException e){
            e.printStackTrace();
        }

        return null;
    }

    @Override
    public void close() {

    }
}
