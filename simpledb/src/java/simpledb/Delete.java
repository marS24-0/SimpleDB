package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId transaction;
    private OpIterator child;
    private TupleDesc td;
    private boolean deleted = false;
    
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        transaction = t;
        this.child = child;
        td = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {"Inserted records"});
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
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
    	if (!deleted) {
        	Tuple t = new Tuple(td);
        	int deletedTuples = 0;
        	
            while(child.hasNext()) {
            	Tuple n = child.next();
            	try {
    				Database.getBufferPool().deleteTuple(transaction, n);
    				deletedTuples++;
    			} catch (IOException e) {
    				e.printStackTrace();
    			};
            }
            deleted = true;
        	t.setField(0, new IntField(deletedTuples));
        	return t;
    	}
    	return null;    	
	}

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return null;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
    }

}
