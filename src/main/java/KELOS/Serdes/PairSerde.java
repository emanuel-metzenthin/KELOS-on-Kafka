package KELOS.Serdes;

import KELOS.Cluster;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.ArrayList;
import java.util.Map;


public class PairSerde implements Serde<Pair<Cluster, Integer>> {

    private Serializer<Pair<Cluster, Integer>> serializer;
    private Deserializer<Pair<Cluster, Integer>> deserializer;

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
    public Serializer<Pair<Cluster, Integer>> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<Pair<Cluster, Integer>> deserializer() {
        return deserializer;
    }
}
