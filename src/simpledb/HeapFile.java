package simpledb;

import org.omg.SendingContext.RunTime;

import java.io.*;
import java.lang.reflect.Array;
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
    private HeapPage[] hps;
    private int pno;

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
        int pageSize = BufferPool.getPageSize();
        int pageNum = (int)Math.floor((int) file.length()) / (pageSize);
        byte[] pageBytes = new byte[pageSize];
        ArrayList<HeapPage> hpLst = new ArrayList<>();
        pno = 0;
        try {
            BufferedInputStream buf = new BufferedInputStream(new FileInputStream(f));
            for (int i = 0; i < pageNum; i += 1) {
                buf.read(pageBytes);
                hpLst.add(new HeapPage(new HeapPageId(fileId, pno), pageBytes));
                pageBytes = new byte[pageSize];
                pno += 1;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        hps = hpLst.toArray(new HeapPage[pageNum]);
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
        int pno = pid.pageNumber();
        if (hps[pno] == null) {
           throw new IllegalArgumentException();
        }
        return hps[pno];
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
        return pno;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
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

