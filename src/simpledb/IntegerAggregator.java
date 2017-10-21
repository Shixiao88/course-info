package simpledb;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfindex;
    private final int afield;
    private final Type gbfieldtype;
    private final Op op;
    private ArrayList<int[]> intlist;
    private boolean needGroup;
    private TupleDesc restd;

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
        intlist = new ArrayList<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        int groupedVal = 1;
        int groupedIndex = 0;
        int groupedValTuple  = 0;
        if (tup.getTupleDesc().getFieldType(afield) == Type.INT_TYPE) {
            groupedValTuple = ((IntField) tup.getField(afield)).getValue();
        }
        if (gbfindex == Aggregator.NO_GROUPING) {
           needGroup = false;
           for (int[] ints : intlist) {
               merge(ints, 0, ints[groupedVal], groupedValTuple, op);
               return;
           }
           int[] ints = new int[3];
           merge(ints, groupedValTuple, op);
           intlist.add(ints);
        } else {
            needGroup = true;
            int groupedInTuple = ((IntField) tup.getField(gbfindex)).getValue();
            for (int[] ints : intlist) {
                if (ints[groupedIndex] == groupedInTuple) {
                    merge(ints, groupedInTuple, ints[groupedVal], groupedValTuple, op);
                    return;
                }
            }
            int[] ints = new int[3];
            merge(ints, groupedInTuple, groupedValTuple, op);
            intlist.add(ints);
            return;
        }


        /*
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
                if (tup.getField(gbfindex).equals(t.getField(groupedIndex))) {
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
        }*/
    }

    private void merge(int[] ints, int val, Op op) {
        merge(ints, 0, val, op);
    }

    private void merge(int[] ints, int groupindex, int val, Op op) {
        ints[0] = groupindex;
        ints[1] = val;
        ints[2] = 1;
        if (op.equals(Op.COUNT)) {
            ints[1] = 1;
        }
    }

    private void merge(int[] ints, int groupindex, int val1, int val2, Op op) {
        switch (op) {
            case MAX:
                ints[0] = groupindex;
                ints[1] = Math.max(val1, val2);
                break;
            case MIN:
                ints[0] = groupindex;
                ints[1] = Math.min(val1, val2);
                break;
            case SUM:
                ints[0] = groupindex;
                ints[1] = val1 + val2;
                break;
            case COUNT:
                ints[0] = groupindex;
                ints[1] += 1;
                break;
            case AVG:
                ints[0] = groupindex;
                ints[1] = val1 + val2;
                ints[2] += 1;
                break;
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
        private Iterator<int[]> agit;
        private boolean isopen;
        private TupleDesc td;

        public AggIterator() {
            isopen = false;
            agit = intlist.iterator();
            restd = getTupleDesc();
        }

        public void open() {
            isopen = true;
        }

        @Override
        public boolean hasNext() throws IllegalStateException {
            if (isopen) {
                int l = intlist.size();
                return agit.hasNext();
            } else {
                throw new IllegalStateException ("the iterator is not opened");
            }
        }

        @Override
        public Tuple next() throws  IllegalStateException {
            if (isopen) {
                int[] i = agit.next();
                Tuple nextTuple;
                if (op.equals(Op.AVG)) {
                    nextTuple = new Tuple(restd);
                    if (needGroup) {
                        nextTuple.setField(0, new IntField(i[0]));
                        nextTuple.setField(1, new IntField(i[1] / i[2]));
                    } else {
                        nextTuple.setField(0, new IntField(i[1]/i[2]));
                    }
                } else {
                    nextTuple = new Tuple(restd);
                    if (needGroup) {
                        nextTuple.setField(0, new IntField(i[0]));
                        nextTuple.setField(1, new IntField(i[1]));
                    } else {
                        nextTuple.setField(0, new IntField(i[1]));
                    }
                }
                return nextTuple;
            } else {
                throw new IllegalStateException("the iterator is not opened");
            }
        }

        @Override
        public void rewind() throws IllegalStateException {
            if (isopen) {
                agit = intlist.iterator();
            } else {
                throw new IllegalStateException("the iterator is not opened");
            }
        }

        @Override
        public TupleDesc getTupleDesc() {
            if (needGroup) {
                return new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});
            }
            return new TupleDesc(new Type[]{Type.INT_TYPE});
        }

        @Override
        public void close () {
            isopen = false;
        }
    }

}
