import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Cluster;
import java.util.Map;

public class ClusterSerde implements Serde<Cluster> {

    private Serializer<Cluster> serializer;
    private Deserializer<Cluster> deserializer;

    public ClusterSerde(){
        this.serializer = new ClusterSerializer();
        this.deserializer = new ClusterDeserializer();
    }

    @Override
    public void configure(Map<String, ?> config, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<Cluster<Double>> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<Cluster<Double>> deserializer() {
        return deserializer;
    }
}
