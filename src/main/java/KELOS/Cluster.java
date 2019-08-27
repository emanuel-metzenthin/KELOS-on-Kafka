package KELOS;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.*;

public class Cluster {
    public int size;
    public double[] centroid;
    public double[] linearSums;
    public double[] minimums;
    public double[] maximums;
    public int[] knnIds;

    public int oldSize;
    public double[] oldLinearSums;

    public double density;
    public double minDensityBound;
    public double maxDensityBound;

    public Cluster(int dimensions, int K){
        this.size = 0;
        this.centroid = new double[dimensions];
        this.linearSums = new double[dimensions];
        this.minimums = new double[dimensions];
        this.maximums = new double[dimensions];
        this.knnIds = new int[K];
        this.oldLinearSums = new double[dimensions];
        this.oldSize = 0;

        density = 0;
        minDensityBound = 0;
        maxDensityBound = 0;
    }

    public Cluster(Cluster cluster, int K){
        this.size = this.oldSize = cluster.size;
        this.centroid = new double[cluster.centroid.length];
        this.linearSums = Arrays.copyOf(cluster.linearSums, cluster.linearSums.length);
        this.oldLinearSums = cluster.linearSums;
        this.minimums = new double[cluster.centroid.length];
        this.maximums = new double[cluster.centroid.length];
        this.knnIds = new int[K];

        density = 0;
        minDensityBound = 0;
        maxDensityBound = 0;
    }

    public Cluster(ArrayList<Double> record, int K){
        double[] recordArray = record.stream().mapToDouble(Double::doubleValue).toArray();

        this.centroid = recordArray;
        this.linearSums = recordArray;
        this.minimums = recordArray;
        this.maximums = recordArray;
        this.size = 1;
        this.knnIds = new int[K];
        this.oldLinearSums = new double[record.size()];
        this.oldSize = 0;

        density = 0;
        minDensityBound = 0;
        maxDensityBound = 0;
    }

    public static Cluster createEndOfWindowToken() {
        Cluster cluster = new Cluster(1, 1);

        cluster.size = -1;

        return cluster;
    }

    public static boolean isEndOfWindowToken(Cluster cluster){
        if (cluster == null){
            return false;
        }

        return cluster.size == -1;
    }

    public double distance(ArrayList<Double> record) {
        double sumOfSquares = 0;

        for(int i = 0; i < this.centroid.length; i++){
            double difference = record.get(i) - this.centroid[i];
            sumOfSquares += difference * difference;
        }

        return Math.sqrt(sumOfSquares);
    }

    public double distance(Cluster cluster) {
        if(cluster.centroid.length != this.centroid.length) {
            return 0;
        }

        double sumOfSquares = 0;

        for(int i = 0; i < this.centroid.length; i++){
            double difference = cluster.centroid[i] - this.centroid[i];
            sumOfSquares += difference * difference;
        }

        return Math.sqrt(sumOfSquares);
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
        if (this.size == 0){
            this.size = otherCluster.size;

            for(int i = 0; i < this.linearSums.length; i++){
                this.linearSums[i] = otherCluster.linearSums[i];
                this.centroid[i] = otherCluster.centroid[i];
                this.minimums[i] = otherCluster.minimums[i];
                this.maximums[i] = otherCluster.maximums[i];
            }
        }
        else if (otherCluster.size != 0){
            this.size += otherCluster.size;

            for(int i = 0; i < this.linearSums.length; i++){
                this.linearSums[i] += otherCluster.linearSums[i];
                this.centroid[i] = this.linearSums[i] / this.size;
                this.minimums[i] = Math.min(this.minimums[i], otherCluster.minimums[i]);
                this.maximums[i] = Math.max(this.maximums[i], otherCluster.maximums[i]);
            }
        }
    }

    public void updateMetrics() {
        this.size -= this.oldSize;

        for(int i = 0; i < this.oldLinearSums.length; i++) {
            this.linearSums[i] -= this.oldLinearSums[i];
        }
    }

    public void calculateKNearestNeighbors(KeyValueIterator<Integer, Cluster> clusters, int ownIndex){
        HashMap<Integer, Double> distances = new HashMap<>();
        ArrayList<Integer> keys = new ArrayList<>();

        while (clusters.hasNext()){
            KeyValue<Integer, Cluster> kv = clusters.next();

            if (kv.key != ownIndex){
                Cluster cluster = kv.value;

                double distance = this.distance(cluster);

                distances.put(kv.key, distance);
                keys.add(kv.key);
            }
        }

        keys.sort(new ArrayIndexComparator(distances));

        for (int i = 0; i < this.knnIds.length && i < keys.size(); i++){
            this.knnIds[i] = keys.get(i);
        }
    }

    private class ArrayIndexComparator implements Comparator<Integer> {
        private final HashMap<Integer, Double> hashmap;

        ArrayIndexComparator(HashMap<Integer, Double> hashmap)
        {
            this.hashmap = hashmap;
        }

        @Override
        public int compare(Integer index1, Integer index2)
        {
            return this.hashmap.get(index1).compareTo(this.hashmap.get(index2));
        }
    }
}
