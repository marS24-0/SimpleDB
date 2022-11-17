package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private int tableid;
    private String alias;
    private String textAlias;
    private DbFile dbFile;
    private DbFileIterator iterator;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        this.tid = tid;
        this.reset(tableid, tableAlias);
        // some code goes here, maybe
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        // NOTE: there was not "some code goes here" here
        return Database.getCatalog().getTableName(this.tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        return this.alias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        this.tableid = tableid;
        this.alias = tableAlias;
        if(tableAlias == null){
            this.textAlias = "null";
        }else{
            this.textAlias = tableAlias;
        }
        this.dbFile = Database.getCatalog().getDatabaseFile(this.tableid);
        this.iterator = this.dbFile.iterator(this.tid);
        // some code goes here, maybe
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        this.iterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc tupleDesc = this.dbFile.getTupleDesc();
        int numFields = tupleDesc.numFields();
        Type[] typeAr = new Type[numFields];
        String[] fieldAr = new String[numFields];
        for(int i=0; i<numFields; i++){
            typeAr[i] = tupleDesc.getFieldType(i);
            fieldAr[i] = this.textAlias + "." + tupleDesc.getFieldName(i);
        }
        return new TupleDesc(typeAr, fieldAr);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        return this.iterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException, TransactionAbortedException, DbException {
        return this.iterator.next();
    }

    public void close() {
        this.iterator.close();
    }

    public void rewind() throws DbException, NoSuchElementException, TransactionAbortedException {
        this.iterator.rewind();
    }
}
