package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId transaction;
    private OpIterator child;
    private int tableId;
    private boolean inserted = false;
    private TupleDesc td;
    
    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        transaction = t;
        this.child = child;
        this.tableId = tableId;
        this.td = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {null});
    }

    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        super.open();
    }

    public void close() {
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        inserted = false;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if (!inserted) {
    		int insertedTuples = 0;
            while(child.hasNext()) {
            	Tuple t = child.next();
            	try {
    				Database.getBufferPool().insertTuple(transaction, tableId, t);
    				insertedTuples++;
    			} catch (IOException e) {
    				e.printStackTrace();
    			};
    			
            }
    		inserted = true;
	    	Tuple t = new Tuple(td);
	    	t.setField(0, new IntField(insertedTuples));
	    	return t;
    	}
    	return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        child = children[0];
    }
}
