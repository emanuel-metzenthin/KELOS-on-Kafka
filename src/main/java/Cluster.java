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

    Cluster(double[] record){
        this.centroid = record;
        this.linearSums = record;
        this.minimums = record;
        this.maximums = record;
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
            // Do not update minimums and maximums because they are not needed at this stage
        }
    }
}
