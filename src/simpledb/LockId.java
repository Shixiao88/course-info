package simpledb;

/**
 * Created by Xiao Shi on 2017/10/16.
 */
public interface LockId {

    public enum LockType {
        EXCLUSIVE_LOCK,
        SHARE_LOCK;

        public static LockType getType(int i) {
            if (i == 0) {
                return SHARE_LOCK;
            } else if (i == 1) {
                return EXCLUSIVE_LOCK;
            } else {
                return null;
            }
        }

    }

    public TransactionId getTransactionId();

    public PageId getPageId();

    public LockType getType();
}
