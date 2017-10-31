package simpledb;

import java.io.*;
import java.text.MessageFormat;
import java.util.*;
import java.util.ArrayList;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    public static final int DEAD_LOCK_TIME_OUT = 4000;

    private List<Page> pageList ;
    private int max_page_num;
    public LockControlBoard controlBoard;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     *
     * when launch a new bufferpool, create a new board.
     */
    public BufferPool(int numPages) {
        pageList = Collections.synchronizedList(new LinkedList<Page>());
        max_page_num = numPages;
        controlBoard = new LockControlBoard();
    }
    
    public static int getPageSize() {
      return BufferPool.pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * Lock is detected by time, assuming that withing such time limit,
     * if the lock is not obtained then it is a deadlock, Abort the running
     * transaction (by @throw the TransactionAbortException)
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        for (Page pg : pageList) {
            if (pg.getId().equals(pid)) {
                lockPage(tid, pid, perm);
                moveAroundPage(pg);
                return pg;
            }
        }
        // if the page is not in bufferpool, there must be no lock on it
        DbFile dbfile = Database.getCatalog().getDbFile(pid.getTableId());
        Page noExistPage = dbfile.readPage(pid);
        lockPage(tid, pid, perm);
        if (pageList.size() < max_page_num) {
            pageList.add(pageList.size(), noExistPage);
            return noExistPage;
        } else {
            /*Eviction Policy afterwards */
            evictPage();
            pageList.add(pageList.size(), noExistPage);
            return noExistPage;
        }
    }

    private void lockPage(TransactionId tid, PageId pid, Permissions perm)
            throws DbException, TransactionAbortedException{
        Lock lock;
        if (perm == Permissions.READ_ONLY) {
                lock = new Lock(tid, pid, Lock.LOCKTYPE.SHARE_LOCK);
            } else {
                lock = new Lock(tid, pid, Lock.LOCKTYPE.EXCLUSIVE_LOCK);
            }
        long timeOutLimit = System.currentTimeMillis() + DEAD_LOCK_TIME_OUT;
        while (true) {
            // deal with deadlock with time detection and self-borting
            long timeWaited = timeOutLimit - System.currentTimeMillis();
            if (timeWaited < 0) {
                throw new TransactionAbortedException();
            }
            int res = lock.inilize(pid, controlBoard);
            if (res > 0) {
                return;
            } else {
                continue;
            }
        }
    }

    /**
     * serve for evistion policy
     * whenever a page is get accessed
     * move it to the very last of the list.
     *
     * */
    private void moveAroundPage(Page pg) {
       pageList.remove(pg);
       pageList.add(pageList.size(), pg);
       return;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        controlBoard.closeLockDG(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public synchronized void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return controlBoard.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public synchronized void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        if (commit) {
            for (Page p: pageList) {
                if (p.isDirty() == tid) {
                    DbFile f = Database.getCatalog().getDbFile(p.getId().getTableId());
                    f.writePage(p);
                    p.markDirty(false, tid);
                }
            }
            controlBoard.closeLock(tid);
        }
        // if it is aborted, then delete all dirty page, need to get from disk if
        // it is asked again, which is unchanged
        else {
            List<Page> pageListCopy = Collections.synchronizedList(new LinkedList<>());
            for (Page p : pageList) {
                if (p.isDirty() == null || p.isDirty() != tid) {
                    pageListCopy.add(pageListCopy.size(), p);
                }
            }
            controlBoard.closeLock(tid);
            pageList = pageListCopy;
        }

    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock
     * acquisition is not needed for lab2). May block if the lock cannot
     * be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have
     * been dirtied so that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        DbFile f = Database.getCatalog().getDbFile(tableId);
        ArrayList<Page> plst = f.insertTuple(tid, t);
        for (Page p : plst) {
            p.markDirty(true, tid);
            /* FORCE policy,
            only when Transaction is Aborted should
            write the dirty page back to the disk and unmark the dirty page
            used in TransactionComplete method.
            */
            //f.writePage(p);
            //p.markDirty(false, tid);
        }
        return;
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        int tableid = t.getRecordId().getPageId().getTableId();
        DbFile f = Database.getCatalog().getDbFile(tableid);
        ArrayList<Page> plst = f.deleteTuple(tid, t);
        for (Page p : plst) {
            p.markDirty(true, tid);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for (Page p : pageList) {
            if (p.isDirty() != null) {
                flushPage(p.getId());
            }
        }

    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // only necessary for lab5
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        for (Page pg : pageList) {
            if (pg.getId().equals(pid)) {
                if (pg.isDirty() != null) {
                    DbFile hf = Database.getCatalog().getDbFile(pid.getTableId());
                    hf.writePage(pg);
                    pg.markDirty(false, null);
                }
            }
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     * Eviction Policy :
     *  LRU
     *  implementation detail:
     *  FIFO for arraylist.
     *
     *  add-on Lock feature:
     *  if the page is dirty, because of NO-STEAL policy it is remain in the memory, and check the next page
     *  if the first is clean, then remove it
     *  (I believe that the check lock then delete should be thread safe, so I add a synchronized block to protect it,
     *  is it a check-then-apply mode?)
     *  the lockControlBoard instance will recorded the read-locked, clean evicted page(this I follow the instructions from the class, but I
     *  don't know what use is that)
     *
     *
     *  words from the instructions:
     *  Note that, in general, evicting a clean page that is locked by a running transaction is OK when using NO STEAL,
     *  as long as your lock manager keeps information about evicted pages around,a
     *  and as long as none of your operator implementations keep references to Page objects which have been evicted.
     */
    private void evictPage() throws DbException {
        Page removePage = null;
        for (Page intentRmPage : pageList) {
            // if the page is not locked by any page or page is not dirty.
            if (intentRmPage.isDirty() == null) {
                removePage = intentRmPage;
                break;
            }
        }
        if (removePage == null) {
            throw new DbException("fail to evit pages");
        } else {
            pageList.remove(removePage);
            controlBoard.recordCleanEvictPage(removePage.getId());
        }
        try {
            flushPage(removePage.getId());
        } catch (IOException e) {
            e.printStackTrace();
        }

        /*
        try {
            while (true) {
                synchronized (this) {
                    Page intentRmPage = pageList.get(0);
                    if (controlBoard.isPageLocked(intentRmPage.getId())) {
                        Thread.sleep(WAIT_TIME);
                        continue;
                    } else {
                        removePage = pageList.remove(0);
                        break;
                    }
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            return;
        }*/

        /*// if the page is the root pointer page in B+Tree, then add to the end of list and delete
        // the next page, as root pointer should not be evicted.
        if (removePage instanceof BTreeRootPtrPage ||
                (removePage instanceof BTreeInternalPage &&
                        ((BTreeInternalPage)removePage).getId().pgcateg() == BTreePageId.ROOT_PTR)) {
            pageList.add(pageList.size()-1, removePage);
            removePage = pageList.remove(0);
        }*/
        /*
        if (removePage.isDirty() != null) {
            try {
                flushPage(removePage.getId());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }*/
    }

}
