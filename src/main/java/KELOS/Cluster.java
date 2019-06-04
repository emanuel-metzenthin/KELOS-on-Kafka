package KELOS;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.*;

public class Cluster {
    public int size;
    public double[] centroid;
    public double[] linearSums;
    public double[] minimums;
    public double[] maximums;
    public int[] knnIds;

    public Cluster(int column_count, int k){
        this.size = 0;
        this.centroid = new double[column_count];
        this.linearSums = new double[column_count];
        this.minimums = new double[column_count];
        this.maximums = new double[column_count];
        this.knnIds = new int[k];
    }

    public Cluster(ArrayList<Double> record, int k){
        double[] recordArray = record.stream().mapToDouble(Double::doubleValue).toArray();

        this.centroid = recordArray;
        this.linearSums = recordArray;
        this.minimums = recordArray;
        this.maximums = recordArray;
        this.size = 1;
        this.knnIds = new int[k];
    }

    public double distance(ArrayList<Double> record) {

        double sum_of_squares = 0;

        for(int i = 0; i < this.centroid.length; i++){
            double difference = record.get(i) - this.centroid[i];
            sum_of_squares += difference * difference;
        }

        return Math.sqrt(sum_of_squares);
    }

    public void addRecord(ArrayList<Double> record){
        this.size += 1;

        for(int i = 0; i < this.linearSums.length; i++){
            this.linearSums[i] += record.get(i);
            this.centroid[i] = this.linearSums[i] / this.size;
            this.minimums[i] = Math.min(this.minimums[i], record.get(i));
            this.maximums[i] = Math.max(this.maximums[i], record.get(i));
        }
    }

    public void merge(Cluster otherCluster){
        this.size += otherCluster.size;
        
        for(int i = 0; i < this.linearSums.length; i++){
            this.linearSums[i] += otherCluster.linearSums[i];
            this.centroid[i] = this.linearSums[i] / this.size;
            this.minimums[i] = Math.min(this.minimums[i], otherCluster.minimums[i]);
            this.maximums[i] = Math.max(this.maximums[i], otherCluster.maximums[i]);
        }
    }

    public void calculateKNearestNeighbors(KeyValueIterator<Integer, Cluster> clusters){
        ArrayList<Double> distances = new ArrayList<>();
        ArrayList<Integer> keys = new ArrayList<>();

        while (clusters.hasNext()){
            KeyValue<Integer, Cluster> cluster = clusters.next();


            Double[] doubleArray = ArrayUtils.toObject(cluster.value.centroid);
            double distance = this.distance((ArrayList<Double>) Arrays.asList(doubleArray));

            distances.add(distance);
            keys.add(cluster.key);
        }

        keys.sort(new ArrayIndexComparator(distances));

        for (int i = 0; i < this.knnIds.length && i < keys.size(); i++){
            this.knnIds[i] = keys.get(i);
        }
    }

    private class ArrayIndexComparator implements Comparator<Integer> {
        private final ArrayList<Double> list;

        ArrayIndexComparator(ArrayList<Double> list)
        {
            this.list = list;
        }

        @Override
        public int compare(Integer index1, Integer index2)
        {
            return list.get(index1).compareTo(list.get(index2));
        }
    }
}
