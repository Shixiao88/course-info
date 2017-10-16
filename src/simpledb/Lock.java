package simpledb;

/**
 * Created by Xiao Shi on 2017/10/16.
 */
public interface Lock {

    public void open();

    public void close(ConcurrencyBoard board, TransactionId tid, PageId pid, LockId lid) throws DbException;

    public LockId getId();
}
