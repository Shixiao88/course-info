package simpledb;

import java.lang.reflect.Array;
import java.util.*;
import java.util.Collections;

/**
 * Created by Xiao Shi on 2017/10/24.
 */
public class LockControlBoard {

    private Map<TransactionId, List<Lock>> tranLockMap;
    private Map<PageId, List<Lock>> pageLockMap;
    private Map<PageId, Lock> pageWriteLockMap;
    /*
    * what is blocked is the handle of specific transaction to specific page
    * i.e. the specific lock
    * if the locks belongs to the same transaction, there should be no block
    * because there is no interleaving in one transaction
    * */
    private List<Lock> blockedHandle;
    private final int BoardSize = 100;

    public LockControlBoard() {
        tranLockMap = Collections.synchronizedMap(new HashMap<>());
        pageLockMap = Collections.synchronizedMap(new HashMap<>());
        pageWriteLockMap = Collections.synchronizedMap(new HashMap<PageId, Lock>());
        blockedHandle = Collections.synchronizedList(new ArrayList<>());
    }

    /**
     * add a lock given transaction id, page id, type of the lock.
     * when to block? :
     *      when in different transaction and accessing a write-lock page, then this lock is blocked.
     *
     * @return 1 if successfully locked, add into trans-lock map, page-lock map,
     * @return -1 if blocked, add tid into blocked transactionId list,
     * @throw transactionAbortException
     */
    public synchronized int addLock(TransactionId tid, PageId pageid, Lock lock)
            throws TransactionAbortedException, DbException {
        if (!(tid.equals(lock.getTid())) || !(pageid.equals(lock.getPageid()))) {
            throw new DbException("add lock error: lock doesn't correspoding to transaction or page ID");
        }
        // if the page has write lock
        Lock writeLockOnPage = getPageWriteLock(pageid);
        if (writeLockOnPage != null) {
            // check if this writelock has same transaction Id with the lock to add,
            // if so, just return 1
            if (writeLockOnPage.getTid().equals(tid)) {
                return 1;
            }
            // if the writelock and lock to add belongs to different transaction
            // this lock is blocked
            blockedHandle.add(blockedHandle.size(), lock);
            return -1;
        }
        // if page does not have lock or has only read lock
        List<Lock> locksOnTrans = tranLockMap.get(tid);
        if (locksOnTrans == null) {
            locksOnTrans = Collections.synchronizedList(new ArrayList<>());
        }
        locksOnTrans.add(lock);
        tranLockMap.put(tid, locksOnTrans);

        List<Lock> locksOnPage = pageLockMap.get(pageid);
        if (locksOnPage == null) {
            locksOnPage = Collections.synchronizedList(new ArrayList<>());
        }
        locksOnPage.add(lock);
        pageLockMap.put(pageid, locksOnPage);
        // if the lock is write lock, then check the page's read lock
        // if page only have read lock generated by same transaction or
        // no lock at all, add the lock, return 1
        // otherwise it is blocked and return -1
        if (lock.getType() == Lock.LOCKTYPE.EXCLUSIVE_LOCK) {
            for (Lock l : locksOnPage) {
                if (!(l.getTid().equals(tid))) {
                    return -1;
                }
            }
            pageWriteLockMap.put(pageid, lock);
        }
        return 1;
    }


    private Lock getPageWriteLock(PageId pid) {
        return pageWriteLockMap.get(pid);
    }

    public boolean isPageLocked(PageId pid){
        return (pageLockMap.containsKey(pid)) || (pageLockMap.get(pid).size() == 0);
    }

    /**
     * remove a lock from transaction and page that it holds;
     * then check the
     * */
    public synchronized void closeLock (TransactionId tid, PageId pageid, Lock lock) throws TransactionAbortedException,
    DbException {
        if (!(tid.equals(lock.getTid())) || !(pageid.equals(lock.getPageid()))) {
            throw new DbException("close lock error: lock does not corresponding to transaction or page ID");
        }
        if (lock.getType() == Lock.LOCKTYPE.EXCLUSIVE_LOCK) {
            enableBlockedHandle(pageid);
        }
        List<Lock> locksOnTrans = tranLockMap.get(tid);
        if (locksOnTrans != null) {
            locksOnTrans.remove(lock);
            tranLockMap.put(tid, locksOnTrans);
        }
        List<Lock> locksOnPage = pageLockMap.get(pageid);
        if (locksOnPage != null) {
            locksOnPage.remove(lock);
            pageLockMap.put(pageid, locksOnPage);
        }
    }

    public synchronized void closeLock (TransactionId tid) {
        List<Lock> locksOnTrans = tranLockMap.get(tid);
        if (locksOnTrans != null) {
           for (Lock l : locksOnTrans) {
               try {
                   closeLock(tid, l.getPageid(), l);
               } catch (DbException | TransactionAbortedException e) {
                   e.printStackTrace();
               }
           }
        }
    }


    /**
     * this method violate the lock rule, it is only used for testing!!
     * */
    public synchronized void closeLockDG(TransactionId tid, PageId pageid) {
        List<Lock> locksOnPage = pageLockMap.get(pageid);
        if (locksOnPage == null) {
            return;
        }
        List<Lock> locksOmTrans = tranLockMap.get(tid);
        if (locksOmTrans == null) {
            return;
        }
        List<Lock> locksOnPageCopy = Collections.synchronizedList(new ArrayList<>());
        List<Lock> locksOnTransCopy = Collections.synchronizedList(new ArrayList<>());
        for (Lock l : locksOnPage) {
            if (!(locksOmTrans.contains(l))) {
                locksOnPageCopy.add(l);
                locksOnTransCopy.add(l);
            }
        }
        pageLockMap.put(pageid, locksOnPageCopy);
        tranLockMap.put(tid, locksOnTransCopy);
        Lock writeLockOnPage = getPageWriteLock(pageid);
        if (writeLockOnPage.getTid().equals(tid)) {
            pageWriteLockMap.remove(pageid);
        }
        return;
    }

    /**
     * when a page is released by write lock, notify this function to enable the lock that is in waiting list;
     * blocked locks is a LIFO queue in waiting line
     * */
    public void enableBlockedHandle (PageId pageid) throws DbException,
    TransactionAbortedException {
        for (Lock l : blockedHandle) {
            if (l.getPageid().equals(pageid)) {
                addLock(l.getTid(), pageid, l);
            }
        }
    }

    public boolean holdsLock (TransactionId tid, PageId pid) {
        List<Lock> locksOnPage = pageLockMap.get(pid);
        if (locksOnPage == null) {
            return false;
        }
        List<Lock> locksOnTran = tranLockMap.get(tid);
        if (locksOnTran == null) {
            return false;
        }
        for (Lock l : locksOnPage) {
            if (locksOnTran.contains(l)) {
                return true;
            }
        }
        return false;
    }

}
