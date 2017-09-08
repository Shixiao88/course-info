package simpledb;

import org.omg.CORBA.INTF_REPOS;

import java.util.ArrayList;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {
    private final int gbfindex;
    private final int afield;
    private final Type gbfieldtype;
    private final Op op;
    private ArrayList<Tuple> tuplelist;
    private boolean isinitiated;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfindex = gbfield;
        this.afield = afield;
        this.gbfieldtype = gbfieldtype;
        this.op = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if (tuplelist.size() == 0) {
            tuplelist.add(tup);
        } else {
            for (Tuple t : tuplelist) {
                if (tup.getField(gbfindex) == t.getField(gbfindex)) {
                    
                }
            }
        }
    }

    private int operation(IntField fd1, IntField fd2, int counter) {
        switch(op) {
            case MAX:
                if (fd1.compare(Predicate.Op.GREATER_THAN_OR_EQ, fd2)) {
                    return fd1.getValue();
                }
                return fd2.getValue();
            case MIN:
                if (fd1.compare(Predicate.Op.LESS_THAN_OR_EQ, fd2)) {
                    return fd1.getValue();
                } return fd2.getValue();
            case AVG:
                return ((fd1.getValue() + fd2.getValue()) / 2);
            case SUM:
                return (fd1.getValue() + fd2.getValue());
            case COUNT:
                return counter++;
        }
        return (Integer)null;
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        throw new UnsupportedOperationException("please implement me for lab2");
    }

}
