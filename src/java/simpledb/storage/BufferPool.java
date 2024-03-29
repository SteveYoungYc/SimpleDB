package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.LockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;
    private final int numPages;
    private final ConcurrentHashMap<Integer, Page> pages;
    private final LockManager lockManager;
    private final LRUReplacer replacer;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        this.pages = new ConcurrentHashMap<>();
        this.lockManager = new LockManager();
        this.replacer = new LRUReplacer(numPages);
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    public void upgradeLock(TransactionId tid, PageId pid) {
        lockManager.upgrade(tid, pid);
    }

    public void releaseSharedLock(TransactionId tid, PageId pid) {
        lockManager.releaseSharedLock(tid, pid);
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        if (perm == Permissions.READ_ONLY) {
            while (!lockManager.acquireSharedLock(tid, pid)) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ignored) {

                }
            }
        } else {
            while (!lockManager.acquireExclusiveLock(tid, pid)) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ignored) {

                }
            }
        }
        Page page;
        if (!pages.containsKey(pid.hashCode())) {
            if (pages.size() >= numPages)
                evictPage();
            DbFile dbfile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            page = dbfile.readPage(pid);
            page.markDirty(false, tid);
            pages.put(pid.hashCode(), page);
            replacer.add(pid.hashCode());
        } else {
            page = pages.get(pid.hashCode());
        }
        return page;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        lockManager.releaseSharedLock(tid, pid);
        lockManager.releaseExclusiveLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        return lockManager.isLocked(tid, p);
    }

    public boolean holdsLock(PageId p) {
        return lockManager.isLocked(p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        if (commit) {
            try {
                flushPages(tid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {    // discard and reread
            for (Page page : pages.values()) {
                if (tid == page.isDirty()) {
                    PageId pid = page.getId();
                    discardPage(pid);
                    DbFile dbfile = Database.getCatalog().getDatabaseFile(pid.getTableId());
                    Page oldPage = dbfile.readPage(pid);
                    oldPage.markDirty(false, tid);
                    pages.put(pid.hashCode(), oldPage);
                    replacer.add(pid.hashCode());
                }
            }
        }
        lockManager.releaseAll(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> list;
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        list = (ArrayList<Page>) file.insertTuple(tid, t);
        for (Page p : list) {
            p.markDirty(true, tid);
            if (pages.containsKey(p.getId().hashCode())) {
                replacer.update(p.getId().hashCode());
            } else {
                if (pages.size() >= numPages)
                    evictPage();
                pages.put(p.getId().hashCode(), p);
                replacer.add(p.getId().hashCode());
            }
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        List<Page> list;
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        list = file.deleteTuple(tid, t);
        for (Page p : list) {
            p.markDirty(true, tid);
            if (pages.containsKey(p.getId().hashCode())) {
                replacer.update(p.getId().hashCode());
            } else {
                if (pages.size() >= numPages)
                    evictPage();
                pages.put(p.getId().hashCode(), p);
                replacer.add(p.getId().hashCode());
            }
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        for (Page page : pages.values()) {
            flushPage(page.getId());
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        pages.remove(pid.hashCode());
        replacer.remove(pid.hashCode());
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // append an update record to the log, with
        // a before-image and after-image.
        Page p = pages.get(pid.hashCode());
        TransactionId dirtier = p.isDirty();
        if (dirtier != null) {
            Database.getLogFile().logWrite(dirtier, p.getBeforeImage(), p);
            Database.getLogFile().force();
        }
        DbFile file = Database.getCatalog().getDatabaseFile(pid.getTableId());
        file.writePage(p);
        pages.get(pid.hashCode()).markDirty(false, null);
        pages.remove(pid.hashCode());
        replacer.remove(pid.hashCode());
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        for (Page page : pages.values()) {
            if (tid == page.isDirty()) {
                flushPage(page.getId());
                page.setBeforeImage();
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        if (pages.isEmpty()) {
            throw new DbException("No page, can not evict!");
        }
        if (pages.size() >= numPages) {
            Optional<Integer> pageEvict = replacer.evict(pages);
            if (pageEvict.isPresent()) {
                pages.remove(pageEvict.get());
            } else {
                throw new DbException("All the pages are dirty!");
            }
        }
    }

}