public class Cluster {
    int size;
    double[] centroid;
    double[] linearSums;
    double[] minimums;
    double[] maximums;

    public double distance(double[] record) {

        double sum_of_squares = 0;

        for(int i = 0; i < record.length; i++){
            double difference = record[i] - this.centroid[i];
            sum_of_squares += difference * difference;
        }

        return Math.sqrt(sum_of_squares);
    }

    public void addRecord(double[] record){
        this.size += 1;

        for(int i = 0; i < record.length; i++){
            this.linearSums[i] += record[i];
            this.centroid[i] = this.linearSums[i] / this.size;
            // Do not update minimums and maximums because they are not needed at this stage
        }
    }
}
