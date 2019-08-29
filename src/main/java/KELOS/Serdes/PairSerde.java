package KELOS.Serdes;

import KELOS.Cluster;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;


public class PairSerde implements Serde<Pair<Cluster, Boolean>> {

    private Serializer<Pair<Cluster, Boolean>> serializer;
    private Deserializer<Pair<Cluster, Boolean>> deserializer;

    public PairSerde(){
        this.serializer = new PairSerializer();
        this.deserializer = new PairDeserializer();
    }

    @Override
    public void configure(Map<String, ?> config, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<Pair<Cluster, Boolean>> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<Pair<Cluster, Boolean>> deserializer() {
        return deserializer;
    }
}
