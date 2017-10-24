package simpledb;

/**
 * Created by Xiao Shi on 2017/10/24.
 */
public class Lock {

    private PageId pageid;
    private TransactionId tid;
    private final LOCKTYPE type;
    private int lockIndex;

    public enum LOCKTYPE {
        SHARE_LOCK, EXCLUSIVE_LOCK;
    }

    public Lock( TransactionId tid, PageId pageid, LOCKTYPE type) {
        this.pageid = pageid;
        this.tid = tid;
        this.type = type;
    }

    /**
     * @return 1 if lock sucess,
     * @return -1 of lock is blocked;
     * */
    public int inilize(PageId pid, LockControlBoard board) throws TransactionAbortedException, DbException {
        if (!(pid.equals(this.pageid))) {
            throw new DbException("lock inilize error");
        }
        return board.addLock(tid, pageid, this);
    }

    public LOCKTYPE getType() {
        return type;
    }


    public void close(PageId pid, LockControlBoard board) throws TransactionAbortedException, DbException {
        if (!(pid.equals(this.pageid))) {
            throw new DbException("lock close error");
        }
        board.closeLock (tid, pageid, this);
    }

    public TransactionId getTid() {
        return tid;
    }

    public PageId getPageid() {
        return pageid;
    }

    public int getLockIndex() {
        return lockIndex;
    }

    /**
     * upgrade the read lock to write lock if the page is not excluded locked.
     * */
    public int upgrate() {
        return -1;
    }
}
