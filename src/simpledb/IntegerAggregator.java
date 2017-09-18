package simpledb;

import org.omg.CORBA.INTF_REPOS;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.ListIterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {
    private final int gbfindex;
    private final int afield;
    private final Type gbfieldtype;
    private final Op op;
    private ArrayList<Tuple> tuplelist;

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
        tuplelist = new ArrayList<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        boolean hasGroup = false;
        TupleDesc globalTupleDecs = tup.getTupleDesc();
        int groupedVal = 1;
        int groupedIndex = 0;
        if (tuplelist.size() == 0 ) {
            if (op == Op.COUNT) {
                TupleDesc restd = new TupleDesc(new Type[]{globalTupleDecs.getFieldType(gbfindex), Type.INT_TYPE});
                Tuple newtuple = new Tuple(restd);
                newtuple.setField(groupedVal, new IntField(1));
                newtuple.setField(groupedIndex, tup.getField(gbfindex));
                tuplelist.add(newtuple);
            } else {
                TupleDesc restd = new TupleDesc(new Type[]
                        {globalTupleDecs.getFieldType(gbfindex), globalTupleDecs.getFieldType(afield)});
                Tuple newtuple = new Tuple(restd);
                newtuple.setField(gbfindex, tup.getField(gbfindex));
                newtuple.setField(afield, tup.getField(afield));
                tuplelist.add(newtuple);
                }
        } else {
            for (Tuple t : tuplelist) {
                TupleDesc tddebug = t.getTupleDesc();
                if (tup.getField(gbfindex).equals(t.getField(groupedIndex))) {
                    /* debug */
                    TupleDesc tdec = t.getTupleDesc();
                    int n = tdec.numFields();
                    /* End debug */
                    hasGroup = true;
                    IntField fd1 = (IntField)tup.getField(afield);
                    IntField fd2 = (IntField)t.getField(groupedVal);
                    switch(op) {
                    case MAX:
                        if (fd1.compare(Predicate.Op.GREATER_THAN_OR_EQ, fd2)) {
                            t.setField(groupedVal, fd1);
                        } break;
                    case MIN:
                         if (fd1.compare(Predicate.Op.LESS_THAN_OR_EQ, fd2)) {
                             t.setField(groupedVal, fd1);
                         } break;
                    case AVG:
                         t.setField(groupedVal, new IntField((fd1.getValue() + fd2.getValue()) / 2));
                         break;
                    case SUM:
                         t.setField(groupedVal, new IntField(fd1.getValue() + fd2.getValue()));
                         break;
                    case COUNT:
                         int counter = fd2.getValue() + 1;
                         t.setField(groupedVal, new IntField(counter));
                         break;
                    }
                }
            }
            if (!hasGroup) {
                if (op == Op.COUNT) {
                    TupleDesc restd = new TupleDesc(new Type[]{globalTupleDecs.getFieldType(gbfindex), Type.INT_TYPE});
                    Tuple newtuple = new Tuple(restd);
                    newtuple.setField(groupedVal, new IntField(1));
                    newtuple.setField(groupedIndex, tup.getField(gbfindex));
                    TupleDesc debugtd = newtuple.getTupleDesc();
                    tuplelist.add(newtuple);
                } else {
                    TupleDesc restd = new TupleDesc(new Type[]
                        {globalTupleDecs.getFieldType(gbfindex), globalTupleDecs.getFieldType(afield)});
                    Tuple res = new Tuple(restd);
                    res.setField(0, tup.getField(gbfindex));
                    res.setField(1, tup.getField(afield));
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
        private Iterator<Tuple> agit;
        private boolean isopen;
        private TupleDesc td;

        public AggIterator() {
            isopen = false;
            agit = tuplelist.iterator();
        }

        public void open() {
            isopen = true;
        }

        @Override
        public boolean hasNext() throws IllegalStateException {
            if (isopen) {
                return agit.hasNext();
            } else {
                throw new IllegalStateException ("the iterator is not opened");
            }
        }

        @Override
        public Tuple next() throws  IllegalStateException {
            if (isopen) {
                Tuple t = agit.next();
                TupleDesc td = t.getTupleDesc();
                int s = td.numFields();
                return agit.next();
            } else {
                throw new IllegalStateException("the iterator is not opened");
            }
        }

        @Override
        public void rewind() throws IllegalStateException {
            if (isopen) {
                agit = tuplelist.iterator();
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
