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

        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(bais);

        try{
            int dimensions = in.readInt();
            Cluster c = new Cluster(dimensions);
            c.size = in.readInt();


            c.centroid = new double[dimensions];
            c.linearSums = new double[dimensions];
            c.minimums = new double[dimensions];
            c.maximums = new double[dimensions];

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
                c.minimums[i] = element;
            }
            for(int i = 0; i < dimensions; i++){
                double element = in.readDouble();
                c.maximums[i] = element;
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
