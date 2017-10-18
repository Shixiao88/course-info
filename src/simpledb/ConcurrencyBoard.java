package simpledb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.*;

/**
 * Created by Xiao Shi on 2017/10/16.
 */

/**
 * the ConcurencyBoard keeps track of two pairs
 * 1. trasaction -- group of locks it holds, must be thread safe
 * 2. page -- group of locks it has, must be thread safe;
 *    the group of lockes also must be thread safe.
 * 3. page -- exclusive lock flag, must be thread safe
 * 4. on-hold transaction, must be thread safe
 */
public class ConcurrencyBoard {
    private final Map<TransactionId, ArrayList<LockId>> tran_lock_map;
    private final Map<PageId, ArrayList<LockId>> page_lock_map;
    private final Map<PageId, Boolean> page_ifExcludeLock_map;
    private final List<TransactionId> on_hold_tid;


    public ConcurrencyBoard() {
        tran_lock_map = Collections.synchronizedMap(new HashMap<>());
        page_lock_map = Collections.synchronizedMap(new HashMap<>());
        page_ifExcludeLock_map = Collections.synchronizedMap(new HashMap<>());
        on_hold_tid = Collections.synchronizedList(new ArrayList<>());
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
     *  if the page has exclusive page, return 1, put the transaction in waiting queue
     *  else return 0
     * */
    public int addPageLock (TransactionId tid, PageId pid, LockId lid) throws DbException {
        if (!(lid.getPageId().equals(pid)) || !(lid.getTransactionId().equals(tid))) {
            throw new DbException("page is not corresponding to lock");
        }
        if (page_lock_map.containsKey(pid)) {
            ArrayList<LockId> lids = page_lock_map.get(pid);
            boolean isPageExcLocked = page_ifExcludeLock_map.get(pid);
            if (!isPageExcLocked) {
                lids.add(lid);
                if (lid.getType().equals(LockId.LockType.EXCLUSIVE_LOCK)) {
                    page_ifExcludeLock_map.put(pid, true);
                }
                return 0;
            } else {
                on_hold_tid.add(tid);
                return 1;
            }
        } else{
            List <LockId> lids = Collections.synchronizedList(new ArrayList<>());
            lids.add(lid);
            if (lid.getType().equals(LockId.LockType.EXCLUSIVE_LOCK)) {
                page_ifExcludeLock_map.put(pid, true);
            } else {
                page_ifExcludeLock_map.put(pid, false);
            }
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
