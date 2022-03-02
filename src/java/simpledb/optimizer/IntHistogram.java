package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.HashMap;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private final int buckets;
    private final int min;
    private final int max;
    private int sum;
    private final int bucketLen;
    // private final HashMap<Integer, Integer> data;
    private final int[] data;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = buckets;
        this.min = min;
        this.max = max;
        this.sum = 0;
        this.bucketLen = (int) Math.ceil((double) (max - min + 1) / buckets);
        this.data = new int[(int) Math.ceil((double) (max - min + 1) / this.bucketLen)];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int idx = (v - min) / bucketLen;
        if (idx < 0 || idx >= data.length) {
            return;
        }
        data[idx]++;
        sum++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
    	// some code goes here
        double sel = 0;
        int idx = (v - min) / bucketLen;
        int subSum = 0;
        switch (op) {
            case EQUALS -> {
                if (idx < 0 || idx >= data.length)
                    return 0;
                sel = ((double) data[idx] / bucketLen / sum);
            }
            case NOT_EQUALS -> {
                if (idx < 0 || idx >= data.length)
                    return 1;
                sel = (1 - (double) data[idx] / bucketLen / sum);
            }
            case LESS_THAN -> {
                for (int i = 0; i < idx && i < data.length; i++) {
                    subSum += data[i];
                }
                if (idx >= 0 && idx < data.length)
                    subSum += data[idx] * (double) (v % bucketLen) / bucketLen;
                sel = (double) subSum / sum;
            }
            case LESS_THAN_OR_EQ -> {
                for (int i = 0; i < idx && i < data.length; i++) {
                    subSum += data[i];
                }
                if (idx >= 0 && idx < data.length)
                    subSum += data[idx] * (double) (v % bucketLen + 1) / bucketLen;
                sel = (double) subSum / sum;
            }
            case GREATER_THAN -> {
                for (int i = Math.max(idx + 1, 0); i < data.length; i++) {
                    subSum += data[i];
                }
                if (idx >= 0 && idx < data.length)
                    subSum += data[idx] * (double) (bucketLen - v % bucketLen - 1) / bucketLen;
                sel = (double) subSum / sum;
            }
            case GREATER_THAN_OR_EQ -> {
                for (int i = Math.max(idx + 1, 0); i < data.length; i++) {
                    subSum += data[i];
                }
                if (idx >= 0 && idx < data.length)
                    subSum += data[idx] * (double) (bucketLen - v % bucketLen) / bucketLen;
                sel = (double) subSum / sum;
            }
        }
        return sel;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity() {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
