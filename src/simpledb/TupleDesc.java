package simpledb;


import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private int tditmeNum;
    private TDItem[] tditmes;

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return new TDiterator();
    }

    private class TDiterator<TDItem> implements Iterator{
        private int index;

        public TDiterator() {
            this.index = 0;
        }

        @Override
        public boolean hasNext() {
            return index < tditmeNum;
        }

        @Override
        public TDItem next() {
            return (TDItem)tditmes[index++];
        }
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        this.tditmeNum = typeAr.length;
        ArrayList<TDItem> tds = new ArrayList<>();
        if (fieldAr != null) {
            for (int i = 0; i < tditmeNum; i += 1) {
                tds.add(new TDItem(typeAr[i], fieldAr[i]));
            }
        }
        this.tditmes = tds.toArray(new TDItem[1]);
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        this.tditmeNum = typeAr.length;
        ArrayList<TDItem> tds = new ArrayList<>();
        for (int i = 0; i < tditmeNum; i += 1) {
            tds.add( new TDItem(typeAr[i], ""));
        }
        this.tditmes = tds.toArray(new TDItem[1]);
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return tditmeNum;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i < 0 || i >= tditmeNum) {
            throw new IndexOutOfBoundsException("index out of bound");
        }
        if (tditmes[i] == null) {
            throw new NoSuchElementException();
        }
        return tditmes[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i < 0 || i >= tditmeNum) {
            throw new IndexOutOfBoundsException("index out of bound");
        }
        if (tditmes[i] == null) {
            throw new NoSuchElementException();
        }
        return tditmes[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int i = 0 ; i < tditmeNum; i += 1) {
            if (tditmes[i].fieldName.equals(name)) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        return tditmeNum * (tditmes[0].fieldType.getLen());
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        int s1 = td1.tditmeNum;
        int s2 = td2.tditmeNum;
        int merge = s1 + s2;
        ArrayList<Type> tps = new ArrayList<>();
        String[] mergedName = new String[merge];
        for (int i = 0; i < s1; i += 1) {
            tps.add(td1.getFieldType(i));
            mergedName[i] = td1.getFieldName(i);
        }
        for (int i = s1; i < merge; i += 1) {
            tps.add(td2.getFieldType(i - s1));
            mergedName[i] = td2.getFieldName(i - s1);
        }
        Type[] mergedType = tps.toArray(new Type[1]);
        return new TupleDesc(mergedType, mergedName);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if (o == null) {
            return false;
        } else if (this == o) {
            return true;
        } else if (!(o instanceof TupleDesc)) {
            return false;
        } else {
            if (getSize() != ((TupleDesc)o).getSize()) {
                return false;
            } else {
                int size = tditmeNum;
                for (int i = 0; i < size; i += 1) {
                    if (!getFieldType(i).equals(((TupleDesc)o).getFieldType(i)) ||
                            !getFieldName(i).equals(((TupleDesc)o).getFieldName(i))) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        String s = "";
        for (int i = 0; i < tditmeNum; i += 1) {
            s += tditmes[i].toString();
        }
        return s;
    }
}
