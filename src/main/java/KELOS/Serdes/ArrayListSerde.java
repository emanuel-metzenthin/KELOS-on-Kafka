package KELOS.Serdes;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.ArrayList;
import java.util.Map;

public class ArrayListSerde implements Serde<ArrayList<Double>> {

    private Serializer<ArrayList<Double>> serializer;
    private Deserializer<ArrayList<Double>> deserializer;

    public ArrayListSerde(){
        this.serializer = new ArrayListSerializer();
        this.deserializer = new ArrayListDeserializer();
    }

    @Override
    public void configure(Map<String, ?> config, boolean isKey) {

    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<ArrayList<Double>> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<ArrayList<Double>> deserializer() {
        return deserializer;
    }
}
