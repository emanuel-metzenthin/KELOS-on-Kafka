package KELOS.Serdes;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.ArrayList;
import java.util.Map;


public class PairSerde implements Serde<Pair<Integer, ArrayList<Double>>> {

    private Serializer<Pair<Integer, ArrayList<Double>>> serializer;
    private Deserializer<Pair<Integer, ArrayList<Double>>> deserializer;

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
    public Serializer<Pair<Integer, ArrayList<Double>>> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<Pair<Integer, ArrayList<Double>>> deserializer() {
        return deserializer;
    }
}
