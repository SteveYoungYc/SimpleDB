package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.LockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc td;
    private final LockManager lockManager;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
        this.lockManager = Database.getBufferPool().lockManager;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here

        int tableId = pid.getTableId();
        int pgNo = pid.getPageNumber();

        RandomAccessFile f;
        try {
            f = new RandomAccessFile(file, "r");
            if ((long) (pgNo + 1) * BufferPool.getPageSize() > f.length()) {
                f.close();
                throw new IllegalArgumentException(String.format("table %d page %d is invalid", tableId, pgNo));
            }
            byte[] bytes = new byte[BufferPool.getPageSize()];
            f.seek((long) pgNo * BufferPool.getPageSize());
            // big end
            int read = f.read(bytes, 0, BufferPool.getPageSize());
            if (read != BufferPool.getPageSize()) {
                throw new IllegalArgumentException(String.format("table %d page %d read %d bytes", tableId, pgNo, read));
            }
            HeapPageId id = new HeapPageId(pid.getTableId(), pid.getPageNumber());
            return new HeapPage(id, bytes);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        PageId pid = page.getId();
        int pgNo = pid.getPageNumber();

        RandomAccessFile f;
        try {
            f = new RandomAccessFile(file, "rw");
            f.seek((long) pgNo * BufferPool.getPageSize());
            f.write(page.getPageData(), 0, BufferPool.getPageSize());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil((int) (file.length() / BufferPool.getPageSize()));
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        int pgNo;
        int numPages = numPages();
        HeapPageId pageId;
        HeapPage page;
        ArrayList<Page> list = new ArrayList<>();
        for (pgNo = 0; pgNo < numPages; pgNo++) {
            pageId = new HeapPageId(getId(), pgNo);
            page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
            if (page.getNumEmptySlots() > 0) {
                Database.getBufferPool().upgradeLock(tid, page.getId());
                page.insertTuple(t);
                page.markDirty(true, tid);
                list.add(page);
                break;
            }
            Database.getBufferPool().releaseSharedLock(tid, page.getId());
        }
        if(list.isEmpty()) {
            pageId = new HeapPageId(getId(), pgNo);
            page = new HeapPage(pageId, HeapPage.createEmptyPageData());
            writePage(page);
            page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
            page.insertTuple(t);
            page.markDirty(true, tid);
            list.add(page);
        }
        return list;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        HeapPageId pageId = (HeapPageId) t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        ArrayList<Page> list = new ArrayList<>();
        page.deleteTuple(t);
        page.markDirty(true, tid);
        list.add(page);
        return list;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            private HeapPage currPage;
            private HeapPage nextPage;
            private int pgNo;
            private PageId pageId;
            private int numPages;
            private Iterator<Tuple> it;
            private boolean switchPage;
            private int detector = 1;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                pgNo = 0;
                pageId = new HeapPageId(getId(), pgNo);
                numPages = numPages();
                currPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
                if (currPage == null) {
                    throw new DbException("No page");
                }
                it = currPage.iterator();
                switchPage = false;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (currPage == null) {
                    return false;
                }
                if (it == null) {
                    throw new DbException("Not open");
                }
                if (it.hasNext()) {
                    return true;
                } else if (pgNo + 1 < numPages) {
                    detector = 1;
                    while (pgNo + detector < numPages) {
                        pageId = new HeapPageId(getId(), pgNo + detector);
                        nextPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
                        it = nextPage.iterator();
                        if (it.hasNext()) {
                            switchPage = true;
                            return true;
                        }
                        detector++;
                    }
                    return false;
                } else {
                    return false;
                }
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (currPage == null || it == null) {
                    throw new NoSuchElementException();
                }
                if (switchPage) {
                    pgNo += detector;
                    currPage = nextPage;
                    switchPage = false;
                }
                return it.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            @Override
            public void close() {
                it = null;
            }
        };
    }

}

