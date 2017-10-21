package simpledb;

import java.text.ParseException;
import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    private byte header[];
    private Tuple[] tuples;
    private int numTuple;
    private boolean isDirty;
    private TransactionId dirtytid;

    byte[] oldData;
    private final Byte oldDataLock=new Byte((byte)0);

    private int realTupleNum;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numTuple = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i = 0; i < header.length; i++) {
            header[i] = dis.readByte();
        }
        this.realTupleNum = numTuple - getNumEmptySlots();
        tuples = new Tuple[numTuple];

        try {
            int numField = td.numFields();
            // allocate and read the actual records of this page
            for (int i = 0; i < realTupleNum; i++) {
                Tuple t = new Tuple(td);
                for (int j = 0; j < numField; j += 1) {
                    if (td.getFieldType(j) == Type.INT_TYPE) {
                        t.setField(j, Type.INT_TYPE.parse(dis));
                    } else {
                        t.setField(j, Type.STRING_TYPE.parse(dis));
                    }
                }
                t.setRecordId(new RecordId(pid, i));
                tuples[i] = t;
            }
        }
        catch(NoSuchElementException | ParseException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {
        return (int)(Math.floor(((double)BufferPool.getPageSize() * 8 / (td.getSize() * 8 + 1))));
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        double n = Math.ceil((double)getNumTuples() / 8);
        int n1 = (int) n;
        return n1;
    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
        oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        return pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (int i=0; i<header.length; i++) {
            try {
                dos.writeByte(header[i]);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);

                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page;  the tuple should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        int num_tuple = getNumTuples();
        for (int i = 0; i < num_tuple; i += 1) {
            if (t.equals(tuples[i]) && isSlotUsed(i)) {
                markSlotUsed(i, false);
                realTupleNum -= 1;
                return;
            }
        } throw new DbException ("tuple not found in this page");
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        int num_slot = getNumTuples();
        if (getNumEmptySlots() > 0) {
            for (int i = 0; i < num_slot; i += 1) {
                if (!(isSlotUsed(i))) {
                    markSlotUsed(i, true);
                    tuples[i] = t;
                    realTupleNum += 1;
                    t.setRecordId(new RecordId(pid, i));
                    return;
                }
            }
        } else throw new DbException("page is full");
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        isDirty = dirty;
        dirtytid = tid;
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        if (isDirty) {
            return dirtytid;
        }
        return null;      
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        int counter = 0;
        int num = getHeaderSize();
        for (int i = 0; i < num; i += 1) {
            for (int j = 0; j < 8; j += 1) {
                counter += ((header[i] >>> j) & 0x1);
            }
        }
        return (numTuple - counter);
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        int byteIndex = i / 8;
        int offset = i % 8;
        byte b = 0x1;
        return (((header[byteIndex] >> offset) & b) == 1);
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        int byteindex = i / 8;
        int offset = i % 8;
        byte b = header[byteindex];
        if (value) {
            header[byteindex] = (byte) (b | (0x1 << offset));
        } else {
            header[byteindex] = (byte) (b & (~(0x1 << offset)));
        }
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public TIterator iterator() {
        return new TIterator();
    }

    private class TIterator implements Iterator<Tuple> {
        int index;
        int counter;
        int tuplelen;

        public TIterator() {
            index = 0;
            counter = 0;
            tuplelen = tuples.length;
        }

        public boolean hasNext() {
            while (index < (tuplelen - 2)) {
                if (!(isSlotUsed(index))) {
                    index ++;
                } else {
                    return true;
                }
            }
            if (index < tuplelen && isSlotUsed(index)) {
                return true;
            }
            return false;
        }

        public Tuple next() {
            Tuple t = tuples[index];
            t.resetTupleDesc(td);
            index += 1;
            counter += 1;
            return t;
        }

        public void close() {}
   }

}

