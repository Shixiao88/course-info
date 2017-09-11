package simpledb;

import jdk.internal.org.objectweb.asm.util.TraceAnnotationVisitor;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min).  Note that we only support aggregates over a single column, grouped
 * by a single column.
 */
public class Aggregate extends Operator {
    private Aggregator agg;
    private DbIterator aggiter;
    private DbIterator feed;

    /**
     * Constructor.  
     *
     *  Implementation hint: depending on the type of afield, you will want to construct an 
     *  IntAggregator or StringAggregator to help you with your implementation of readNext().
     * 
     *
     * @param child The DbIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if there is no grouping
     * @param aop The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
        this.feed = child;
        Type type = child.getTupleDesc().getFieldType(afield);
        // integer aggregator
        if (type == Type.INT_TYPE) {
            agg = new IntegerAggregator(gfield, Type.INT_TYPE, afield, aop);
        // string aggregator
        } else if (type == Type.STRING_TYPE) {
            agg = new StringAggregator(gfield, Type.STRING_TYPE, afield, aop);
        }
        try {
            while (feed.hasNext()) {
                agg.mergeTupleIntoGroup(feed.next());
            }
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }
        aggiter = agg.iterator();
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        switch (aop) {
        case MIN:
            return "min";
        case MAX:
            return "max";
        case AVG:
            return "avg";
        case SUM:
            return "sum";
        case COUNT:
            return "count";
        }
        return "";
    }

    public void open()
        throws NoSuchElementException, DbException, TransactionAbortedException {
        try {
            aggiter.open();
        } catch (NoSuchElementException | DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the next tuple.  If there is a group by field, then 
     * the first field is the field by which we are
     * grouping, and the second field is the result of computing the aggregate,
     * If there is no group by field, then the result tuple should contain
     * one field representing the result of the aggregate.
     * Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        try {
            if (aggiter.hasNext()) {
                return aggiter.next();
            }
            return null;
        } catch (TransactionAbortedException | DbException e) {
            e.printStackTrace();
            throw e;
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
        aggiter.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate.
     * If there is no group by field, this will have one field - the aggregate column.
     * If there is a group by field, the first field will be the group by field, and the second
     * will be the aggregate value column.
     * 
     * The name of an aggregate column should be informative.  For example:
     * "aggName(aop) (child_td.getFieldName(afield))"
     * where aop and afield are given in the constructor, and child_td is the TupleDesc
     * of the child iterator. 
     */
    public TupleDesc getTupleDesc() {
        return aggiter.getTupleDesc();
    }

    public void close() {
        aggiter.close();
    }
}
