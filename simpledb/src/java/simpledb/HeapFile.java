package simpledb;

import java.io.*;
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
    private File file;
    private TupleDesc tupleDesc;

    /**
        * Constructs a heap file backed by the specified file.
        * 
        * @param f
        *            the file that stores the on-disk backing store for this heap
        *            file.
        */
    public HeapFile(File f, TupleDesc td) {
        this.file = f;
        this.tupleDesc = td;
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
        * you will need to generate this tableid somewhere to ensure that each
        * HeapFile has a "unique id," and that you always return the same value for
        * a particular HeapFile. We suggest hashing the absolute file name of the
        * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
        * 
        * @return an ID uniquely identifying this HeapFile.
        */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }

    /**
        * Returns the TupleDesc of the table stored in this DbFile.
        * 
        * @return TupleDesc of this DbFile.
        */
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException {
    	int tableId = pid.getTableId();
        int pgNo = pid.getPageNumber();
        byte[] rawPgData = HeapPage.createEmptyPageData();

        // random access read from disk
        try {
          FileInputStream in = new FileInputStream(file);
          in.skip(pgNo * BufferPool.getPageSize());
          in.read(rawPgData);
          return new HeapPage(new HeapPageId(tableId, pgNo), rawPgData);
        } catch (IOException e) {
          throw new IllegalArgumentException("Heap file I/O error");
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
    	// Implemented as BTreeFile.writePage
		PageId pid = page.getId();
		byte[] data = page.getPageData();
		RandomAccessFile rf = new RandomAccessFile(file, "rw");
		rf.seek(BufferPool.getPageSize() + (pid.getPageNumber()-1) * BufferPool.getPageSize());
		rf.write(data);
		rf.close();
    }

    /**
        * Returns the number of pages in this HeapFile.
        */
    public int numPages() {
//        return this.nPages;
    	int fileSize = (int) file.length();
		return fileSize / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t) // TODO
            throws DbException, IOException, TransactionAbortedException {
    	ArrayList<Page> modifiedPages = new ArrayList<>();
    	boolean pageFound = false;
    	
    	for (int i = 0; i < numPages() && !pageFound; i++) {
    		HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_WRITE);
    		if (p.getNumEmptySlots() > 0) {
    			pageFound = true;
    			p.insertTuple(t);
    			p.markDirty(true, tid);
    			modifiedPages.add(p);
    		}
    	}
    	
    	if (!pageFound) {
    		HeapPageId newPageId = new HeapPageId(getId(), numPages());
    		HeapPage newPage = new HeapPage(newPageId, new byte[BufferPool.getPageSize()]);
    		writePage(newPage);
    		HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, newPageId, Permissions.READ_WRITE);
            p.insertTuple(t);
            p.markDirty(true, tid);
            modifiedPages.add(newPage);
    	}
    	return modifiedPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
    	boolean pageFound = false;
    	
    	PageId tuplePageId = t.getRecordId().getPageId();
    	HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, tuplePageId, Permissions.READ_WRITE);
    	if (p == null)
    		throw new DbException("");
    	
    	p.deleteTuple(t);
    	p.markDirty(true, tid);
    	ArrayList<Page> pageList = new ArrayList<Page>();
    	pageList.add(p);
    	return pageList;
    }
    
    private class HeapFileIterator implements DbFileIterator {

        private Integer pageCursor;
        private Iterator<Tuple> tupleIterator;
        private final TransactionId transactionId;
        private final int tableId;
        private final int numPages;

        public HeapFileIterator(TransactionId tid) {
          this.pageCursor = null;
          this.tupleIterator = null;
          this.transactionId = tid;
          this.tableId = getId();
          this.numPages = numPages();
        }

        private Iterator<Tuple> getTupleIterator(int pageNumber) throws TransactionAbortedException, DbException {
          PageId pid = new HeapPageId(tableId, pageNumber);
          return ((HeapPage) Database.getBufferPool().getPage(transactionId, pid, Permissions.READ_ONLY)).iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
          pageCursor = 0;
          tupleIterator = getTupleIterator(pageCursor);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
          if (pageCursor != null) {
            while (pageCursor < numPages - 1) {
              if (tupleIterator.hasNext()) {
                return true;
              } else {
                pageCursor += 1;
                tupleIterator = getTupleIterator(pageCursor);
              }
            }
            return tupleIterator.hasNext();
          } else {
            return false;
          }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
          if (hasNext()) {
            return tupleIterator.next();
          }
          throw new NoSuchElementException("HeapFileIterator error: no more elements");
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
          close();
          open();
        }

        @Override
        public void close() {
          pageCursor = null;
          tupleIterator = null;
        }

      }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
//        return new HeapFileIterator(this, tid);
        return new HeapFileIterator(tid);
    }

}

