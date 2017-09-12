package simpledb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private static TupleDesc td;
    private Field[] fields;
    private int num;
    private RecordId rid;

    /**
     * Create a new tuple with the specified schema (type).
     * 
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        this.td = td;
        this.num = td.numFields();
        this.fields = new Field[num];
        ArrayList<Field> fds = new ArrayList<>();
        for (int i = 0; i < num; i += 1) {
            if (td.getFieldType(i) == Type.INT_TYPE) {
                fds.add(new IntField(0));
            } else {
                fds.add(new StringField("", 0));
            }
        }
        this.fields = fds.toArray(new Field[1]);
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        return this.td;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        if (rid != null) {
            return rid;
        }
        return null;
    }

    /**
     * Set the RecordId information for this tuple.
     * 
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        this.rid = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     * 
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        if (i < 0 || i >= num) {
            throw new IndexOutOfBoundsException("index out of range");
        }
        fields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     * 
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        if (i < 0 || i >= num) {
            throw new IndexOutOfBoundsException("index out of range");
        }
        return fields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * 
     * column1\tcolumn2\tcolumn3\t...\tcolumnN\n
     * 
     * where \t is any whitespace, except newline, and \n is a newline
     */
    public String toString() {
        String s = "";
        for (int i = 0; i < num - 1; i += 1) {
            s += fields[i].toString();
            s += "\t";
        }
        s += fields[num-1].toString();
        s += "\n";
        return s;
    }
    
    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields() {

        return new FIterator();
    }

    private class FIterator<Field> implements Iterator {
        private int index;

        public FIterator() {
            index = 0;
        }

        @Override
        public boolean hasNext() {
            return index < num;
        }

        @Override
        public Field next() {
            return (Field)fields[index ++];
        }
    }
    
    /**
     * reset the TupleDesc of this tuple
     * */
    public void resetTupleDesc(TupleDesc td) {
        this.td = td;
    }
}
