package simpledb.storage;

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

    public synchronized void add(int pageHash) {
        queue.add(pageHash);
    }

    public synchronized void remove(int pageHash) {
        for (int p : queue.stream().toList()) {
            if (p == pageHash) {
                queue.remove(p);
                return;
            }
        }
    }

    public synchronized void update(int pageHash) {
        for (int p : queue.stream().toList()) {
            if (p == pageHash) {
                queue.remove(p);
                queue.add(p);
                return;
            }
        }
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
