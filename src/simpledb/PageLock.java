package simpledb;

/**
 * Created by Xiao Shi on 2017/10/16.
 */

/**
 *  the lock based on pages.
 * */
public class PageLock implements Lock {
    private PageLockId pagelockid;
    private boolean isOpen;


    public PageLock(PageLockId plid) {
        this.pagelockid = plid;
        isOpen = false;
    }

    public void open() {
        isOpen = true;
    }

    /**
     * close itself, the clean work is done in ConcurrencyBoard.
     * */
    public void close(ConcurrencyBoard board, TransactionId tid, PageId pid, LockId lid)
            throws DbException {
        isOpen = false;
        board.cleanLocks(tid, pid, lid);
    }

    public LockId getId() {
        return pagelockid;
    }


}
