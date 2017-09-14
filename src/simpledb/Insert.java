package simpledb;
import java.io.IOException;
import java.util.*;

/**
 * Inserts tuples read from the child operator into
 * the tableid specified in the constructor
 */
public class Insert extends Operator {
    private TransactionId tid;
    private DbIterator feediter;
    private int tableId;
    private DbFileIter hpiter;
    private boolean isDone;

    /**
     * Constructor.
     * @param t The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
        throws DbException, TransactionAbortedException {
        TupleDesc tdchild = child.getTupleDesc();
        TupleDesc tdtableid = Database.getCatalog().getTupleDesc(tableid);
        if (tdchild.equals(tdtableid)) {
            this.tid = t;
            this.feediter = child;
            this.tableId = tableid;
            HeapFile hpfile = (HeapFile) Database.getCatalog().getDbFile(tableid);
            hpiter = (DbFileIter)hpfile.iterator(tid);
            isDone = false;
        } else {
            throw new DbException("tuple description is not correspondent");
        }
    }

    public TupleDesc getTupleDesc() {
        Type[] t = new Type[] {Type.INT_TYPE};
        return new TupleDesc(t);
    }

    public void open() throws DbException, TransactionAbortedException {
        feediter.open();
        hpiter.open();
    }

    public void close() {
        feediter.close();
        hpiter.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        feediter.rewind();
        hpiter.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool.
     * An instances of BufferPool is available via Database.getBufferPool().
     * Note that insert DOES NOT need check to see if a particular tuple is
     * a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
    * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext()
            throws TransactionAbortedException, DbException {
        if (isDone) {
            return null;
        }
        int counter = 0;
        while (feediter.hasNext()) {
            Tuple t = feediter.next();
            try {
                Database.getBufferPool().insertTuple(tid, tableId, t);
                counter += 1;
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }
        Type[] typearry = new Type[] {Type.INT_TYPE};
        Tuple res = new Tuple(new TupleDesc(typearry));
        res.setField(0, new IntField(counter));
        isDone = true;
        return res;
    }
}
