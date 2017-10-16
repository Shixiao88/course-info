package simpledb;

import com.sun.security.sasl.util.AbstractSaslImpl;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by Xiao Shi on 2017/10/16.
 */

/**
 * the ConcurencyBoard keeps track of two pairs
 * 1. trasaction -- group of locks
 * 2. page -- group of locks
 * 3. on-hold page
 * 4. on-hold transaction
 */
public class ConcurrencyBoard {
    private HashMap<TransactionId, ArrayList<LockId>> tran_lock_map;
    private HashMap<PageId, ArrayList<LockId>> page_lock_map;
    private ArrayList<LockId> onHoldLocks;


    public ConcurrencyBoard() {
        tran_lock_map = new HashMap<>();
        page_lock_map = new HashMap<>();
        onHoldLocks = new ArrayList<>();
    }

    /**
     * the verification of Share/Exclusive Lock is verified previously
     * this function is applied after the valid verification.
     * */
    public void addTranLockPair(TransactionId tid, LockId lid) {
        if (tran_lock_map.containsKey(tid)) {
            if (tran_lock_map.get(tid).contains(lid)) {
                return;
            }
            tran_lock_map.get(tid).add(lid);
        } else {
            ArrayList<LockId> lids = new ArrayList<>();
            lids.add(lid);
            tran_lock_map.put(tid, lids);
        }
    }

    public void delTranLockPair (TransactionId tid, LockId lid) {
        if (tran_lock_map.containsKey(tid)) {
            ArrayList<LockId> lids = tran_lock_map.get(tid);
            if (lids.size() <= 1) {
                tran_lock_map.remove(tid);
            } else {
                lids.remove(lid);
            }
        }
    }

    /**
     *  one page can only have one exclusive lock,
     *  if add lock to page fail, return -1, else return 0.
     * */
    public int addPageLockPair(PageId pid, LockId lid) {
        if (page_lock_map.containsKey(pid)) {
            ArrayList<LockId> lids = page_lock_map.get(pid);
            for (LockId lockid : lids) {
                if (lockid.getType() == LockId.LockType.EXCLUSIVE_LOCK) {
                    return -1;
                }
            }
            lids.add(lid);
            return 0;
        } else{
            ArrayList<LockId> lids = new ArrayList<>();
            lids.add(lid);
            return 0;
        }
    }

    public void delPageLockPair(PageId pid, LockId lid) {
        if (page_lock_map.containsKey(pid)) {
            ArrayList<LockId> lids = page_lock_map.get(pid);
            if (lids.size() <= 1) {
                page_lock_map.remove(pid);
                return;
            } else {
                lids.remove(pid);
                return;
            }
        }
    }

    /**
     *  record the locks that on hold because the object has already have
     *  an exclusive lock
     * */
    public void putLockOnHold (LockId lid) {
        if (onHoldLocks.contains(lid)) {
            return;
        }
        onHoldLocks.add(onHoldLocks.size(), lid);
    }

    /**
     * if successfully remove the lockid, return it, otherwise return null
     * */
    public LockId releaseLockOnHold (LockId lid) {
        if (onHoldLocks.contains(lid)) {
            onHoldLocks.remove(lid);
            return lid;
        }
        return null;
    }

    public ArrayList<LockId> getLockIdByPid(PageId pid) {
        if (page_lock_map.containsKey(pid)) {
            return page_lock_map.get(pid);
        }
        return new ArrayList<LockId>();
    }


    /**
     * necessary work after close specific lock
     * */
    public void cleanLocks(TransactionId tid, PageId pid, LockId lid)
            throws DbException {
        if ((!(tid.equals(lid.getTransactionId()))) || (!(pid.equals(lid.getPageId())))) {
            throw new DbException("Transaction, Page and Lock id not correspondent");
        }
        delTranLockPair(tid, lid);
        delPageLockPair(pid, lid);
    }
}
