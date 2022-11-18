package simpledb;
import java.nio.Buffer;
import java.security.NoSuchAlgorithmException;
import java.util.*;

public class HeapFileIterator implements DbFileIterator {
    private HeapFile hf;
    private TransactionId tid;
    private int nextPageNumber;
    private Iterator<Tuple> iterator;
    private BufferPool buffer;
    public HeapFileIterator(HeapFile hf, TransactionId tid){
        this.hf = hf;
        this.tid = tid;
        this.buffer = Database.getBufferPool();
        this.close();
    }
    public void open() throws DbException, TransactionAbortedException {
        if(this.hf.numPages() == 0){
            throw new DbException("No pages");
        }
        this.nextPageNumber = 0;
        this.nextIterator();
    }
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return this.iterator != null && (this.iterator.hasNext() || this.nextPageNumber < this.hf.numPages());
    }
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if(!this.hasNext()){
            throw new NoSuchElementException();
        }
        if(!this.iterator.hasNext()){
            this.nextIterator();
        }
        return this.iterator.next();
    }
    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
    }
    public void close() {
        this.nextPageNumber = 0;
        this.iterator = null;
    }
    private void nextIterator() {
        HeapPageId pid = new HeapPageId(this.hf.getId(), this.nextPageNumber++);
        try{
            HeapPage hp = (HeapPage) this.buffer.getPage(this.tid, pid, null);
            this.iterator = hp.iterator();
        }catch(TransactionAbortedException exception){
        }catch(DbException exception){
        }
    }
}
