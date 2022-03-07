package simpledb.storage;

import simpledb.common.DbException;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public class LRUReplacer {
    int numPages;
    private final ConcurrentLinkedQueue<Integer> queue;

    public LRUReplacer(int numPages) {
        this.numPages = numPages;
        this.queue = new ConcurrentLinkedQueue<>();
    }

    public int getSize() {
        return queue.size();
    }

    public synchronized void add(int pageHash) {
        if (!queue.contains(pageHash))
            queue.offer(pageHash);
    }

    public synchronized void remove(int pageHash) {
        for (int p : queue.stream().toList()) {
            if (p == pageHash) {
                queue.remove(p);
                return;
            }
        }
    }

    public synchronized void update(int pageHash) throws DbException {
        for (int p : queue.stream().toList()) {
            if (p == pageHash) {
                queue.remove(p);
                queue.offer(p);
                return;
            }
        }
        throw new DbException("LRUReplacer update failed!");
    }

    public synchronized Optional<Integer> evict(ConcurrentHashMap<Integer, Page> pages) {
        for (int p : queue.stream().toList()) {
            if (pages.get(p).isDirty() != null)
                continue;
            queue.remove(p);
            return Optional.of(p);
        }
        return Optional.empty();
    }
}
