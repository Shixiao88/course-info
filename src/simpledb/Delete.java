package simpledb;

import java.io.IOException;

/**
 * The delete operator.  Delete reads tuples from its child operator and
 * removes them from the table they belong to.
 */
public class Delete extends Operator {
    private TransactionId tid;
    private DbIterator feed;
    private boolean isDone;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.tid = t;
        this.feed = child;
        this.isDone = false;
    }

    public TupleDesc getTupleDesc() {
        return feed.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        this.feed.open();
    }

    public void close() {
        this.feed.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        this.feed.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if (isDone) {
            return null;
        }
        int counter = 0;
        while(feed.hasNext()) {
            Tuple t = feed.next();
            try {
                Database.getBufferPool().deleteTuple(tid, t);
                counter++;
            } catch (DbException e) {
                continue;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        Type[] typearry = new Type[] {Type.INT_TYPE};
        Tuple res = new Tuple(new TupleDesc(typearry));
        res.setField(0, new IntField(counter));
        isDone = true;
        return res;
    }
}
