package KELOS.Serdes;

import KELOS.Cluster;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.ArrayList;
import java.util.Map;


public class ClusterListSerde implements Serde<ArrayList<Cluster>> {

    private Serializer<ArrayList<Cluster>> serializer;
    private Deserializer<ArrayList<Cluster>> deserializer;

    public ClusterListSerde(){
        this.serializer = new ClusterListSerializer();
        this.deserializer = new ClusterListDeserializer();
    }

    @Override
    public void configure(Map<String, ?> config, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<ArrayList<Cluster>> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<ArrayList<Cluster>> deserializer() {
        return deserializer;
    }
}
