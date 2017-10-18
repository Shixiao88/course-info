package simpledb;

import javax.xml.soap.SAAJMetaFactory;

/**
 * Created by Xiao Shi on 2017/10/16.
 */
public class PageLockId implements LockId {
    private  TransactionId tid;
    private PageId pid;
    private PageLock pagelock;
    private LockType locktype;

    /*public class Locktype implements LockType {
        private int type;

        private Locktype(int type) {
            this.type = type;
        }

        public String toString() {
            if (this.type == 0) {
                return "Share Lock";
            } else if (this.type == 1) {
                return "Exclusive Lock";
            }
            return "Unkwon Lock Type";
        }
    }

    public final Locktype SHARE_LOCK = new Locktype(0);
    public final Locktype EXCLUSIVE_LOCK = new Locktype(1);*/


    public PageLockId(TransactionId tid, PageId pid, Permissions permission) {
        this.tid = tid;
        this.pid = pid;
        if (permission == Permissions.READ_ONLY) {
            locktype = LockType.SHARE_LOCK;
        } else {
            locktype = LockType.EXCLUSIVE_LOCK;
        }

    }

    @Override
    public boolean equals(Object that) {
        if (that == null) return false;
        else if (that == this) return true;
        else if (!(that instanceof PageLockId)) return false;
        else {
            PageLockId thatid = (PageLockId)that;
            return (this.tid.equals(thatid.getTransactionId()) && this.pid.equals(((PageLockId) that).getPageId()));
        }
    }

    public TransactionId getTransactionId() {
        return tid;
    }

    public PageId getPageId() {
        return pid;
    }

    public LockType getType() {
        return locktype;
    }

}
