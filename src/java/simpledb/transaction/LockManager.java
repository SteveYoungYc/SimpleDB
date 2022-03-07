package simpledb.transaction;

import simpledb.common.Database;
import simpledb.storage.PageId;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LockManager {
    // TODO: Is there any more efficient way to store the meta data?
    private final ConcurrentHashMap<Integer, Set<Integer>> sharedLockSets;
    private final ConcurrentHashMap<Integer, Set<Integer>> exclusiveLockSets;
    private final ConcurrentHashMap<Integer, Set<Integer>> graph;
    private final ConcurrentHashMap<Integer, Integer> vertices;
    private int v;

    public LockManager() {
        this.exclusiveLockSets = new ConcurrentHashMap<>();
        this.sharedLockSets = new ConcurrentHashMap<>();
        this.graph = new ConcurrentHashMap<>();
        this.vertices = new ConcurrentHashMap<>();
        this.v = 0;
    }

    /**
     * Add to the dependency graph and detect if there is a loop in the graph
     *
     * @param tid1 the txn that wants to acquire a lock
     * @param tranHash2 the txn that holds the lock
     */
    private synchronized void addToGraph(TransactionId tid1, int tranHash2) throws TransactionAbortedException {
        int tranHash1 = tid1.hashCode();
        Set<Integer> set;
        if (graph.containsKey(tranHash1)) {
            set = graph.get(tranHash1);
            set.add(tranHash2);
        } else {
            set = new HashSet<>();
            set.add(tranHash2);
            graph.put(tranHash1, set);
        }
        if (!vertices.containsKey(tranHash1)) {
            vertices.put(tranHash1, v);
            v++;
        }
        if (!vertices.containsKey(tranHash2)) {
            vertices.put(tranHash2, v);
            v++;
        }
        Digraph digraph = new Digraph(v);
        for (int t1 : graph.keySet()) {
            for (int t2 : graph.get(t1)) {
                digraph.addEdge(vertices.get(t1), vertices.get(t2));
            }
        }
        DirectedCycle directedCycle = new DirectedCycle(digraph);
        if (directedCycle.hasCycle()) {
            Database.getBufferPool().transactionComplete(tid1, false);
            throw new TransactionAbortedException();
        }
    }

    public synchronized boolean acquireSharedLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        Set<Integer> set;
        int tranHash = tid.hashCode();
        int pageHash = pid.hashCode();

        for (int key : exclusiveLockSets.keySet()) {
            if (exclusiveLockSets.get(key).contains(pageHash) && key != tranHash) {
                addToGraph(tid, key);
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

    public synchronized boolean acquireExclusiveLock(TransactionId tid, PageId pid) throws TransactionAbortedException {
        Set<Integer> set;
        int tranHash = tid.hashCode();
        int pageHash = pid.hashCode();

        for (int key : sharedLockSets.keySet()) {
            if (sharedLockSets.get(key).contains(pageHash) && key != tranHash) {
                addToGraph(tid, key);
                return false;
            }
        }
        for (int key : exclusiveLockSets.keySet()) {
            if (exclusiveLockSets.get(key).contains(pageHash) && key != tranHash) {
                addToGraph(tid, key);
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

    public synchronized void releaseSharedLock(TransactionId tid, PageId pid) {
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

    public synchronized void releaseExclusiveLock(TransactionId tid, PageId pid) {
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
        if (sharedLockSets.containsKey(tranHash)) {
            for (int p : sharedLockSets.get(tranHash)) {
                sharedLockSets.remove(p);
            }
            sharedLockSets.remove(tranHash);
        }
        if (exclusiveLockSets.containsKey(tranHash)) {
            for (int p : exclusiveLockSets.get(tranHash)) {
                exclusiveLockSets.remove(p);
            }
            exclusiveLockSets.remove(tranHash);
        }
    }

    public synchronized void upgrade(TransactionId tid, PageId pid) {
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

    public synchronized boolean isLocked(TransactionId tid, PageId pid) {
        boolean lockedBySharedLock = false, lockedByExclusiveLock = false;
        if (sharedLockSets.containsKey(tid.hashCode()))
            lockedBySharedLock = sharedLockSets.get(tid.hashCode()).contains(pid.hashCode());
        if (exclusiveLockSets.containsKey(tid.hashCode()))
            lockedByExclusiveLock = exclusiveLockSets.get(tid.hashCode()).contains(pid.hashCode());
        return lockedBySharedLock || lockedByExclusiveLock;
    }

    public synchronized boolean isLocked(PageId pid) {
        for (Set<Integer> set : sharedLockSets.values()) {
            if (set.contains(pid.hashCode()))
                return true;
        }
        for (Set<Integer> set : exclusiveLockSets.values()) {
            if (set.contains(pid.hashCode()))
                return true;
        }
        return false;
    }

    public synchronized void release(PageId pid) {
        for (Set<Integer> set : sharedLockSets.values()) {
            set.remove(pid.hashCode());
        }
        for (Set<Integer> set : exclusiveLockSets.values()) {
            set.remove(pid.hashCode());
        }
    }
}
