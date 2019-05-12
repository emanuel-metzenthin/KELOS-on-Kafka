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
    public Cluster deserialize(String s, byte[] bytes) {

        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(bais);
        Cluster c = new Cluster();

        try{
            c.size = in.readInt();
            int dimensions = in.readInt();

            c.linearSums = new double[dimensions];
            c.minimums = new double[dimensions];
            c.maximums = new double[dimensions];

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
        } catch (IOException e){
            e.printStackTrace();
        }

        return c;
    }

    @Override
    public void close() {

    }
}
