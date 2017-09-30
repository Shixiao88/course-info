package simpledb;

import org.omg.CORBA.TRANSACTION_MODE;

import javax.xml.crypto.Data;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Created by Xiao Shi on 2017/8/24.
 */
public class HPFileIter implements DbFileIterator {

    private int pageIndexInThisFile;
    private TransactionId tid;
    private int fileId;
    private HeapPageId pid;
    private boolean isOpened = false;
    private Iterator<Tuple> iter;
    private HeapFile f;
    private HeapPageId initialId;

    public HPFileIter(TransactionId tid, int tableId) {
        try {
            this.tid = tid;
            this.fileId = tableId;
            pageIndexInThisFile = 0;
            pid = new HeapPageId(fileId, pageIndexInThisFile);
            initialId = new HeapPageId(fileId, 0);
            f = (HeapFile) Database.getCatalog().getDbFile(fileId);
            HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            iter = page.iterator();
            iter = ((HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY)).iterator();
        } catch (TransactionAbortedException | DbException e) {
            e.printStackTrace();
        }
    }

    /**
     * Opens the iterator
     * @throws DbException when there are problems opening/accessing the database.
     */
    @Override
    public void open()
        throws DbException, TransactionAbortedException {
            isOpened = true;
    }

    /** @return true if there are more tuples available. */
    public boolean hasNext()
        throws DbException, TransactionAbortedException {
        // if there is no page in the file and in the file there is no more tuple to iterate.
        // then it has no next value
        if (!isOpened) { return false;}
        while (pageIndexInThisFile < f.numPages()) {
            if (iter.hasNext()) {
                return true;
            }
            pageIndexInThisFile ++;
            if (pageIndexInThisFile == f.numPages()) {
                return false;
            }
            HeapPage nextpage = (HeapPage)Database.getBufferPool().getPage(
                    tid, new HeapPageId(fileId, pageIndexInThisFile), Permissions.READ_ONLY);
            iter = nextpage.iterator();
        }
        return false;
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
        if (isOpened) {
            try {
                if (iter.hasNext()) {
                    return iter.next();
                }
                pageIndexInThisFile++;
                pid = new HeapPageId(fileId, pageIndexInThisFile);
                iter = ((HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY)).iterator();
                return iter.next();
            } catch (DbException | TransactionAbortedException | NoSuchElementException e) {
                e.printStackTrace();
                throw e;
            }
        } else {
            throw new NoSuchElementException();
        }
    }

    /**
     * Resets the iterator to the start.
     * @throws DbException When rewind is unsupported.
     */
    public void rewind() throws DbException, TransactionAbortedException {
        try {
                pageIndexInThisFile = 0;
                pid = initialId;
                iter = ((HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY)).iterator();
        } catch (DbException | TransactionAbortedException e) {
                e.printStackTrace();
        }
    }

    /**
     * Closes the iterator.
     */

    public void close() {
        isOpened = false;
    }


}
