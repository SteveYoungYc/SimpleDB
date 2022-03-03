package simpledb.transaction;

import simpledb.storage.PageId;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class LockManager {

    private final HashMap<Integer, Set<Integer>> sharedLockSets;
    private final HashMap<Integer, Set<Integer>> exclusiveLockSets;

    public LockManager() {
        this.exclusiveLockSets = new HashMap<>();
        this.sharedLockSets = new HashMap<>();
    }

    public boolean acquireSharedLock(TransactionId tid, PageId pid) {
        Set<Integer> set;
        int tranHash = tid.hashCode();
        int pageHash = pid.hashCode();

        for (int key : exclusiveLockSets.keySet()) {
            if (exclusiveLockSets.get(key).contains(pageHash) && key != tranHash) {
                return false;
            }
        }
        if (sharedLockSets.containsKey(tranHash)) {
            set = sharedLockSets.get(tranHash);
            set.add(pageHash);
        } else {
            set = new HashSet<>();
            set.add(pageHash);
            sharedLockSets.put(tranHash, set);
        }
        return true;
    }

    public boolean acquireExclusiveLock(TransactionId tid, PageId pid) {
        Set<Integer> set;
        int tranHash = tid.hashCode();
        int pageHash = pid.hashCode();

        for (int key : sharedLockSets.keySet()) {
            if (sharedLockSets.get(key).contains(pageHash) && key != tranHash) {
                return false;
            }
        }
        for (int key : exclusiveLockSets.keySet()) {
            if (exclusiveLockSets.get(key).contains(pageHash) && key != tranHash) {
                return false;
            }
        }
        if (exclusiveLockSets.containsKey(tranHash)) {
            set = exclusiveLockSets.get(tranHash);
            set.add(pageHash);
        } else {
            set = new HashSet<>();
            set.add(pageHash);
            exclusiveLockSets.put(tranHash, set);
        }
        return true;
    }

    public void releaseSharedLock(TransactionId tid, PageId pid) {
        Set<Integer> set;
        int tranHash = tid.hashCode();
        int pageHash = pid.hashCode();
        if (!sharedLockSets.containsKey(tranHash))
            return;
        set = sharedLockSets.get(tranHash);
        if (!set.contains(pageHash))
            return;
        set.remove(pageHash);
        if (set.isEmpty())
            sharedLockSets.remove(tranHash);
    }

    public void releaseExclusiveLock(TransactionId tid, PageId pid) {
        Set<Integer> set;
        int tranHash = tid.hashCode();
        int pageHash = pid.hashCode();
        if (!exclusiveLockSets.containsKey(tranHash))
            return;
        set = exclusiveLockSets.get(tranHash);
        if (!set.contains(pageHash))
            return;
        set.remove(pageHash);
        if (set.isEmpty())
            exclusiveLockSets.remove(tranHash);
    }

    public void releaseAll(TransactionId tid) {
        int tranHash = tid.hashCode();
        sharedLockSets.remove(tranHash);
        exclusiveLockSets.remove(tranHash);
    }

    public void upgrade(TransactionId tid, PageId pid) {
        Set<Integer> set;
        int tranHash = tid.hashCode();
        int pageHash = pid.hashCode();
        sharedLockSets.get(tranHash).remove(pageHash);
        if (exclusiveLockSets.containsKey(tranHash)) {
            set = exclusiveLockSets.get(tranHash);
            set.add(pageHash);
        } else {
            set = new HashSet<>();
            set.add(pageHash);
            exclusiveLockSets.put(tranHash, set);
        }
    }

    public boolean isLocked(TransactionId tid, PageId pid) {
        boolean lockedBySharedLock = sharedLockSets.get(tid.hashCode()).contains(pid.hashCode());
        boolean lockedByExclusiveLock = exclusiveLockSets.get(tid.hashCode()).contains(pid.hashCode());
        return lockedBySharedLock || lockedByExclusiveLock;
    }
}
