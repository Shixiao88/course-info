package simpledb;
import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {
    private JoinPredicate jp;
    private DbIterator left_outer;
    private DbIterator right_inner;
    private TupleDesc joined_td;
    private TupleDesc td1;
    private TupleDesc td2;
    private Tuple t_outer;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.  Accepts to children to join and the predicate
     * to join them on
     *
     * @param p The predicate to use to join the children
     * @param child1 Iterator for the left(outer) relation to join
     * @param child2 Iterator for the right(inner) relation to join
     */
    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        jp = p;
        left_outer = child1;
        right_inner = child2;
        TupleDesc td1_outer = left_outer.getTupleDesc();
        td1 = td1_outer;
        TupleDesc td2_innter = right_inner.getTupleDesc();
        td2 = td2_innter;
        joined_td = TupleDesc.merge(td1_outer, td2_innter);
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return jp;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        return td1.getFieldName(jp.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        return td2.getFieldName(jp.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible implementation logic.
     */
    public TupleDesc getTupleDesc() {
        return joined_td;
    }

    public void open()
        throws DbException, NoSuchElementException, TransactionAbortedException {
        try {
            super.open();
            left_outer.open();
            right_inner.open();
            if (left_outer.hasNext()) {
                t_outer = left_outer.next();
            }
        } catch (DbException | NoSuchElementException | TransactionAbortedException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        super.close();
        left_outer.close();
        right_inner.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        try {
            left_outer.rewind();
            right_inner.rewind();
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no more tuples.
     * Logically, this is the next tuple in r1 cross r2 that satisfies the join
     * predicate.  There are many possible implementations; the simplest is a
     * nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of
     * Join are simply the concatenation of joining tuples from the left and
     * right relation. Therefore, if an equality predicate is used 
     * there will be two copies of the join attribute
     * in the results.  (Removing such duplicate columns can be done with an
     * additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     *
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        try {
            while (true) {
                while (right_inner.hasNext()) {
                    Tuple t_inner = right_inner.next();
                    if (jp.filter(t_outer, t_inner)) {
                        Tuple newtuple = new Tuple(joined_td);
                        int num_outer = td1.numFields();
                        int num_total = joined_td.numFields();
                        for (int i = 0; i < num_outer; i += 1) {
                            newtuple.setField(i, t_outer.getField(i));
                        }
                        for (int i = 0; i < num_total - num_outer; i += 1) {
                            newtuple.setField(i + num_outer, t_inner.getField(i));
                        }
                        return newtuple;
                    } else {
                        continue;
                    }
                }
                if (left_outer.hasNext()) {
                    t_outer = left_outer.next();
                    right_inner.rewind();
                } else { break; }
            }
            return null;
        } catch (TransactionAbortedException | DbException e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{left_outer, right_inner};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        left_outer = children[0];
        right_inner = children[1];
    }
}
