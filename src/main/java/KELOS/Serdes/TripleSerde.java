package KELOS.Serdes;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.ArrayList;
import java.util.Map;


public class TripleSerde implements Serde<Triple<Integer, ArrayList<Double>, Long>> {

    private Serializer<Triple<Integer, ArrayList<Double>, Long>> serializer;
    private Deserializer<Triple<Integer, ArrayList<Double>, Long>> deserializer;

    public TripleSerde(){
        this.serializer = new TripleSerializer();
        this.deserializer = new TripleDeserializer();
    }

    @Override
    public void configure(Map<String, ?> config, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<Triple<Integer, ArrayList<Double>, Long>> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<Triple<Integer, ArrayList<Double>, Long>> deserializer() {
        return deserializer;
    }
}
