package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int numTuples;
    private int numBuckets;
    private int min;
    private int max;
    private double interval;
    /* mark the buckets from 0 to numBuckets - 1, the value falls into corresponding interval will fall into
    *  indexed buckets
    *  */
    private Map <Integer, ArrayList<Integer>> histogram;

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
        this.numTuples = 0;
        this.numBuckets = buckets;
        this.min = min;
        this.max = max;
        /*
         * interval maybe float, considering equaly division must be like this.
        * */
        this.interval = (max + 1 - min) / (double)buckets;
        histogram = new HashMap<>();
        for (int i = 0; i < numBuckets; i += 1) {
            histogram.put(i, new ArrayList<Integer>());
        }
    }

    private int getBucketIndexByValue (int v) {
        /*
         * do not consider overflow
         */
        return (int)((v - min) / interval);
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        numTuples += 1;
    	// some code goes here
        /* check v falls into which bucket, and fall into indexed buckets (index 0 ~ numBuckets - 1)
         * if v is less than min or larger than max, create a new range for it;
         * */
        if (v >= min && v <= max) {
            int vIndex = getBucketIndexByValue(v);
            ArrayList<Integer> itemsInBucktes = histogram.get(vIndex);
            itemsInBucktes.add(v);
            histogram.put(vIndex, itemsInBucktes);

            /* if the added value is smaller than current minimum value.
                update the min
                calculate the possible gap between old min and current min (can be several intervals)
                update number of buckets
                update histogram's indexes, in order to avoid interleaves with current histogram,
                create a copy histogram and change the pointer at last;
            * */

        } else if (v < min) {
            HashMap<Integer, ArrayList<Integer>> copyHistogram = new HashMap<>();
            int oldmin = min;
            min = v;
            int numInterval = (int)(Math.ceil(oldmin - v) / interval);
            numBuckets += numInterval;
            ArrayList<Integer> newMinBucket = new ArrayList<>();
            newMinBucket.add(v);
            copyHistogram.put(0, newMinBucket);
            for (int i = 1; i < numInterval; i += 1) {
                copyHistogram.put(i, new ArrayList<>());
            }for (int i = numInterval; i < numBuckets; i += 1) {
                copyHistogram.put(i, histogram.get(i - numInterval));
            }
            histogram = copyHistogram;

            /* if the added value is larger than current max value, same as above situation */
        } else {
            HashMap<Integer, ArrayList<Integer>> copyHisgram = new HashMap<>();
            int oldmax = max;
            max = v;
            int numInterval = (int)(Math.ceil(v - oldmax) / interval);
            numBuckets += numInterval;
            ArrayList<Integer> newMaxBucket = new ArrayList<>();
            newMaxBucket.add(v);
            copyHisgram.put(numBuckets - 1, newMaxBucket);
            for (int i = 0; i < oldmax; i += 1) {
                copyHisgram.put(i, histogram.get(i));
            } for (int i = oldmax; i < numBuckets - 1; i += 1) {
                copyHisgram.put(i, new ArrayList<>());
            }
            histogram = copyHisgram;
        }
    }

    /**
     * Xiao:
     * helper checker function to adjust the number of buckets per the following functions question.
     *
     * */
    private void checkNumBuckets() {

    }

    private double estimateEqualSelectivity(Predicate.Op op, int v) {

        /** Xiao:
         * during unit test found some worth noted:
         * if max - min range is lower than number of bucket,
         * meaning that integer will only fall into some of the buckets, left the rest empty
         * for example:
         * 0 - 31, with 100 buckets.
         * some of the interval will be 0.31 - 0.62, there will be no integer at all in fall into these range.
         *
         * do I need to readjust the number of buckets of ensure that there is no purely float range?
         * or i need to readjust the interval to be 1.0 if it is smaller than 1.0?
         *
         * */

        checkNumBuckets();
        if (v >= min && v <= max) {
            int vIndex = getBucketIndexByValue(v);
            int numItemInBucket = (histogram.get(vIndex)).size();
            double debug = numItemInBucket / interval;
            double debug2 = debug/numTuples;
            if (interval < 1.0) {
                return  (double)numItemInBucket /numTuples;
            }
            return (numItemInBucket / interval) / numTuples;
        } else {
            return 0.0;
        }
    }

    private double estimateLargerSelectivity(Predicate.Op op, int v) {
        checkNumBuckets();
        if (v >= min && v <= max) {
            int vIndex = getBucketIndexByValue(v);
            int numItemInBucket = (histogram.get(vIndex)).size();
            double maxInBucket = min + (vIndex * interval);
            double numInCurrentBucket = numItemInBucket * ((maxInBucket - v) / interval);
            int numLargerBucket = 0;
            for (int i = vIndex + 1; i < numBuckets; i += 1) {
                int numInEachLargerBucket = histogram.get(i).size();
                numLargerBucket += numInEachLargerBucket;
            }
            return (numInCurrentBucket + numLargerBucket) / numTuples;
        } else if (v < min) {
            return 1.0;
        } else {
            return 0.0;
        }
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
        double selectivity = -1.0;
        switch (op) {
            case EQUALS:
                selectivity = estimateEqualSelectivity(op, v);
                break;
            case GREATER_THAN:
                selectivity = estimateLargerSelectivity(op, v);
                break;
            case GREATER_THAN_OR_EQ:
                selectivity = estimateEqualSelectivity(op, v) +
                        estimateLargerSelectivity(op, v);
                break;
            case LESS_THAN:
                selectivity = 1 - estimateLargerSelectivity(op, v) -
                        estimateEqualSelectivity(op, v);
                break;
            case LESS_THAN_OR_EQ:
                selectivity = 1 - estimateLargerSelectivity(op, v);
                break;
            case NOT_EQUALS:
                selectivity = 1 - estimateEqualSelectivity(op, v);
                break;
        }
        return selectivity;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        String s = "";
        for (int i = 0 ; i < numBuckets; i += 1) {
            String line = "index: " + i + ", range: [" + (min + (interval * i)) +
                    " , " + (min + (interval * (i + 1))) + "), fallsIns: {" + histogram.get(i).toString() + "}\n";
            s += line;
        }
        return s;
    }
}
