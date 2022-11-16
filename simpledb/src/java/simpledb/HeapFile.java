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
    private int id;
    private int nPages;

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
        this.id = f.getAbsoluteFile().hashCode();
        this.nPages = (int) f.length() / BufferPool.getPageSize();
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
        return this.id;
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
        if(pid.getPageNumber() >= this.nPages){
            throw new IllegalArgumentException("Page does not exist");
        }
        int pageOffset = pid.getPageNumber() * BufferPool.getPageSize(); // TODO: check getPageNumber
        byte[] data = new byte[BufferPool.getPageSize()];
        try{
            RandomAccessFile randomAccessFile = new RandomAccessFile(this.file, "r");
            randomAccessFile.seek(pageOffset);
            randomAccessFile.read(data);
            randomAccessFile.close();
            // HeapPageId hpid = new HeapPageId(this.id, pid.getPageNumber()); // TODO: something with this line
            return new HeapPage((HeapPageId) pid, data);
        }catch(FileNotFoundException exception){
            throw new IllegalArgumentException("File not found");
        }catch(IOException exception){
            throw new IllegalArgumentException("Unable to read");
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
        return this.nPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new HeapFileIterator(this, tid);
    }

}

