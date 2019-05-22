import java.util.ArrayList;

public class Cluster {
    int size;
    double[] centroid;
    double[] linearSums;
    double[] minimums;
    double[] maximums;

    Cluster(int column_count){
        this.size = 0;
        this.centroid = new double[column_count];
        this.linearSums = new double[column_count];
        this.minimums = new double[column_count];
        this.maximums = new double[column_count];
    }

    Cluster(ArrayList<Double> record){
        double[] recordArray = record.stream().mapToDouble(Double::doubleValue).toArray();

        this.centroid = recordArray;
        this.linearSums = recordArray;
        this.minimums = recordArray;
        this.maximums = recordArray;
        this.size = 1;
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
}
