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
            int off = pno * BufferPool.getPageSize();
            byte[] b = new byte[BufferPool.getPageSize()];
            int readin = raf.read(b, off, BufferPool.getPageSize());
            raf.close();
            if (readin < 0) {
                return null;
            }
            HeapPage hp = new HeapPage((HeapPageId)pid, b);
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
        byte[] data = tuple2Data(t);
        int pno = numPages();
        for (int i = 0; i < pno; i += 1) {
            HeapPage heapPage = (HeapPage)Database.getBufferPool().getPage(
                    tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
            try {
                //find a page with empty slot, insert into this empty slot and write to the file
                heapPage.insertTuple(t);
                insertPages.add(heapPage);
                RandomAccessFile raf = new RandomAccessFile(file, "rw");
                raf.write(data, i * BufferPool.getPageSize(), BufferPool.getPageSize());
                raf.close();
                return insertPages;
            } catch (DbException e) {
                continue;
            }
        }
        // if no page with empty slots, create a new page and append to the end of file
        byte[] newEmptyData = HeapPage.createEmptyPageData();
        for (int i = 0; i < data.length; i += 1) {
            newEmptyData[i] = data[i];
        }
        HeapPage hp = new HeapPage(new HeapPageId(getId(), pno + 1), newEmptyData);
        insertPages.add(hp);
        try {
            FileOutputStream fout = new FileOutputStream(file, true);
            fout.write(newEmptyData);
            fout.close();
            return insertPages;
        } catch (IOException e) {
            throw e;
        }
    }

    private byte[] tuple2Data(Tuple t) {
        TupleDesc td = t.getTupleDesc();
        byte[] data = new byte[td.getSize()];
        int j = 0;
        // convert tuple to byte array
        for (int i = 0; i < td.numFields(); i += 1) {
            if (td.getFieldType(i).equals(Type.INT_TYPE)) {
                // big endian
                data[j++] = (byte)(((IntField)t.getField(i)).getValue() & (0xff << 8));
                data[j++] = (byte)(((IntField)t.getField(i)).getValue() & 0xff);
            } else {
                byte[] b = ((StringField)t.getField(i)).toString().getBytes();
                for (int p = 0; p < Type.STRING_LEN; p += 1) {
                    data[j++] = b[p];
                }
            }
        }
        return data;
    }

    private void changePageArray(HeapPage[] hparray, int size) {
        int len = hparray.length;
        HeapPage[] newarray = new HeapPage[size];
        int maxrange = Math.min(len, size);
        for (int i = 0; i < maxrange; i += 1) {
            newarray[i] = hparray[i];
        }
        hparray = newarray;
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

