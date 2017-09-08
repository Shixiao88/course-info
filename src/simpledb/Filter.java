package simpledb;
import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {
    private final Predicate p;
    private final DbIterator iter;
    /**
     * Constructor accepts a predicate to apply and a child
     * operator to read tuples to filter from.
     *
     * @param p The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        this.p = p;
        this.iter = child;
    }

    public TupleDesc getTupleDesc() {
        return iter.getTupleDesc();
    }

    public void open()
        throws DbException, NoSuchElementException, TransactionAbortedException {
        try {
            iter.open();
        } catch (DbException | NoSuchElementException | TransactionAbortedException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        iter.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        try {
            iter.rewind();
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
        }
    }

    /**
     * AbstractDbIterator.readNext implementation.
     * Iterates over tuples from the child operator, applying the predicate
     * to them and returning those that pass the predicate (i.e. for which
     * the Predicate.filter() returns true.)
     *
     * @return The next tuple that passes the filter, or null if there are no more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext()
        throws NoSuchElementException, TransactionAbortedException, DbException {
        try {
            while (iter.hasNext()) {
                Tuple t = iter.next();
                if (p.filter(t)) {
                    return t;
                }
            }
            return null;
        } catch (NoSuchElementException | TransactionAbortedException | DbException e) {
            e.printStackTrace();
            throw e;
        }
    }
}
