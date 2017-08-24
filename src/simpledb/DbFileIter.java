package simpledb;

/**
 * Created by Xiao Shi on 2017/8/24.
 */
public class DbFileIter implements DbFileIterator {

    private int pageIndexInThisFile;
    private TransactionId tid;
    private int fileId;
    private HeapPageId pid;
    private boolean isOpened = false;
    private HeapFile f;

    public DbFileIter(TransactionId tid, int tableId) {
        this.tid = tid;
        this.fileId = tableId;
    }

    /**
     * Opens the iterator
     * @throws DbException when there are problems opening/accessing the database.
     */
    @Override
    public void open()
        throws DbException, TransactionAbortedException {
        try {
            isOpened = true;
            pageIndexInThisFile = 0;
            pid = new HeapPageId(fileId, pageIndexInThisFile);
            f = (HeapFile)Database.getCatalog().getDatabaseFile(fid);
        } catch (TransactionAbortedException e) {
            e.printStackTrace();
        } catch (DbException e) {
            e.printStackTrace();
        }
    }

    /** @return true if there are more tuples available. */
    public boolean hasNext()
        throws DbException, TransactionAbortedException {
        return pageIndexInThisFile < f.numPages();
    }

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */
    @Override
    public Tuple next()
        throws DbException, TransactionAbortedException, NoSuchElementException {
        pageIndexInThisFile ++;
        pid = new HeapPageId(fileId, pageIndexInThisFile);
        return Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
    }

    /**
     * Resets the iterator to the start.
     * @throws DbException When rewind is unsupported.
     */
    public void rewind() throws DbException, TransactionAbortedException;

    /**
     * Closes the iterator.
     */
    p

}
