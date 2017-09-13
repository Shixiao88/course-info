package simpledb;
import java.util.*;

/**
 * Inserts tuples read from the child operator into
 * the tableid specified in the constructor
 */
public class Insert extends Operator {
    private TransactionId tid;
    private DbIterator dbiter;
    private HeapFile hpfile;
    private DbFileIter hpiter;

    /**
     * Constructor.
     * @param t The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
        throws DbException {
        TupleDesc tdchild = child.getTupleDesc();
        TupleDesc tdtableid = Database.getCatalog().getTupleDesc(tableid);
        if (!(tdchild.equals(tdtableid))) {
            this.tid = t;
            this.dbiter = child;
            hpfile = (HeapFile)Database.getCatalog().getDbFile(tableid);
        } else {
            throw new DbException("tuple description is not correspondent");
        }
    }

    public TupleDesc getTupleDesc() {
        return dbiter.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        dbiter.open();
        hpiter = (DbFileIter)hpfile.iterator(tid);
        hpiter.open();
    }

    public void close() {
        dbiter.close();
        hpiter.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        dbiter.rewind();
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
        if (dbiter.hasNext()) {
            hpfile.insertTuple(tid, dbiter.next());
            return dbiter.next();
        }
        // some code goes here
        return null;
    }
}
