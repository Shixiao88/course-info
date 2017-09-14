package simpledb;

import org.omg.SendingContext.RunTime;

import javax.xml.crypto.Data;
import java.io.*;
import java.lang.reflect.Array;
import java.nio.Buffer;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc td;
    private final int fileId;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.td = td;
        this.fileId = f.getAbsoluteFile().hashCode();
        Database.getCatalog().addTable(this);

    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        return this.fileId;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            int pno = pid.pageNumber();
            raf.seek((long)pno * BufferPool.getPageSize());
            byte[] b = new byte[BufferPool.getPageSize()];
            int readin = raf.read(b);
            raf.close();
            if (readin < 0) {
                b = HeapPage.createEmptyPageData();
            }
            HeapPage hp = new HeapPage((HeapPageId) pid, b);
            return hp;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        int pageSize = BufferPool.getPageSize();
        int pageNum = (int)Math.floor((int) file.length()) / (pageSize);
        return pageNum;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        ArrayList<Page> insertPages = new ArrayList<>();
        int pno = numPages();
        for (int i = 0; i < pno; i += 1) {
            HeapPage heapPage = (HeapPage)Database.getBufferPool().getPage(
                    tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
            try {
                //find a page with empty slot, insert into this empty slot and write to the file
                heapPage.insertTuple(t);
                insertPages.add(heapPage);
                RandomAccessFile raf = new RandomAccessFile(file, "rw");
                raf.seek((long)i * BufferPool.getPageSize());
                raf.write(heapPage.getPageData());
                raf.close();
                return insertPages;
            } catch (DbException e) {
                continue;
            }
        }
        // if no page with empty slots, create a new page and append to the end of file
        byte[] newEmptyData = HeapPage.createEmptyPageData();
        HeapPage hp = new HeapPage(new HeapPageId(getId(), pno + 1), newEmptyData);
        hp.insertTuple(t);
        insertPages.add(hp);
        try {
            FileOutputStream fout = new FileOutputStream(file, true);
            fout.write(hp.getPageData());
            fout.close();
            return insertPages;
        } catch (IOException e) {
            throw e;
        }
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
        TransactionAbortedException {

        PageId pid = t.getRecordId().getPageId();
        Database.getBufferPool().deleteTuple(tid, t);
        Page pg = Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
        return pg;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) throws DbException, TransactionAbortedException {
        try {
            return new DbFileIter(tid, fileId);
        } catch (DbException | TransactionAbortedException e) {
            e.printStackTrace();
            throw e;
        }
    }

}

