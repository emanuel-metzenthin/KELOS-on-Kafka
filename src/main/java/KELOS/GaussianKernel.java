package KELOS;

public class GaussianKernel {

    private double h;

    public GaussianKernel(double bandwidth){
        this.h = bandwidth;
    }

    public double computeDensity(double x){
        double exp = Math.pow(Math.E, - (x * x) / (2 * this.h * this.h));
        double sqrt = Math.sqrt(2 * Math.PI);

        return exp / (sqrt * this.h);
    }
}
