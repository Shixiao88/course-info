package simpledb;

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
    private final int afield;
    private final int groupfield;
    private final Aggregator.Op op;

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
        this.afield = afield;
        this.groupfield = gfield;
        this.op = aop;
        Type type = child.getTupleDesc().getFieldType(gfield);
        // integer aggregator
        if (type == Type.INT_TYPE) {
            agg = new IntegerAggregator(gfield, Type.INT_TYPE, afield, aop);
        // string aggregator
        } else if (type == Type.STRING_TYPE) {
            agg = new StringAggregator(gfield, Type.STRING_TYPE, afield, aop);
        }
        try {
            feed.open();
            while (feed.hasNext()) {
                agg.mergeTupleIntoGroup(feed.next());
            } aggiter = agg.iterator();
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
        // some code goes here
        if (groupfield >= 0) return groupfield;
        return Aggregator.NO_GROUPING;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
        // some code goes here
        if (groupfield >= 0) {
            return feed.getTupleDesc().getFieldName(groupfield);
        }
        return null;
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
        // some code goes here
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
        // some code goes here
        return feed.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return op;
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
        aggiter.open();
        super.open();
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
        super.close();
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{feed};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        feed = children[0];
    }
}
