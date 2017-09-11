package simpledb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.ListIterator;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {
    private final int gbfindex;
    private final int afield;
    private final Type gbfieldtype;
    private final Op op;
    private ArrayList<Tuple> tuplelist;
    private TupleDesc td;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfindex = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;
        tuplelist = new ArrayList<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        boolean hasGroup = false;
        td = tup.getTupleDesc();
        Tuple newtuple = new Tuple(td);
        if (tuplelist.size() == 0) {
            if (op == Op.COUNT) {
                newtuple.setField(gbfindex, tup.getField(gbfindex));
                newtuple.setField(afield, new IntField(1));
            } else {
                newtuple.setField(gbfindex, tup.getField(gbfindex));
                newtuple.setField(afield, tup.getField(afield));
            }
            tuplelist.add(newtuple);
        } else {
            for (Tuple t : tuplelist) {
                if (tup.getField(gbfindex).equals(t.getField(gbfindex))) {
                    hasGroup = true;
                    IntField fd2 = (IntField) t.getField(afield);
                    switch(op) {
                    case COUNT:
                         int counter = fd2.getValue() + 1;
                         t.setField(afield, new IntField(counter));
                         break;
                    }
                }
            }
            if (!hasGroup) {
                Tuple res = new Tuple(td);
                if (op == Op.COUNT) {
                    res.setField(gbfindex, tup.getField(gbfindex));
                    res.setField(afield, new IntField(1));
                    tuplelist.add(res);
                } else {
                    res.setField(gbfindex, tup.getField(gbfindex));
                    res.setField(afield, tup.getField(afield));
                    tuplelist.add(res);
                }
            }
        }
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
     return new AggIterator();
    }

    private class AggIterator implements DbIterator{
        private Iterator<Tuple> aggiter;
        private boolean isopen;
        private TupleDesc td;

        public AggIterator() {
            aggiter = tuplelist.iterator();
            isopen = false;
        }

        public void open() {
            isopen = true;
        }

        @Override
        public boolean hasNext() throws IllegalStateException {
            if (isopen) {
                return aggiter.hasNext();
            } else {
                throw new IllegalStateException ("the iterator is not opened");
            }
        }

        @Override
        public Tuple next() throws  IllegalStateException {
            if (isopen) {
                return aggiter.next();
            } else {
                throw new IllegalStateException("the iterator is not opened");
            }
        }

        @Override
        public void rewind() throws IllegalStateException {
            if (isopen) {
                aggiter = tuplelist.listIterator();
            } else {
                throw new IllegalStateException("the iterator is not opened");
            }
        }

        @Override
        public TupleDesc getTupleDesc() {
            return tuplelist.get(0).getTupleDesc();
        }

        @Override
        public void close () {
            isopen = false;
        }
    }

}
