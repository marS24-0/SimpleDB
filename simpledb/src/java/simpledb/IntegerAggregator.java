package simpledb;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

import javax.xml.stream.XMLStreamConstants;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    
    private TupleDesc td;
    private HashMap<Field, Pair<Integer, Integer>> aggregations = new HashMap<>();
    
    private class Pair<A, B> {
    	private A a;
    	private B b;
    	public Pair(A a, B b) {
    		this.a = a;
    		this.b = b;
    	}
    	public A get1() {
    		return a;
    	}
    	public B get2() {
    		return b;
    	}
    	public void set1(A a) {
    		this.a = a;
    	}
    	public void set2(B b) {
    		this.b = b;
    	}
    }
    
    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	Field groupByField = null;
    	String fieldName = null;
    	
		if (gbfield == Aggregator.NO_GROUPING) {
			groupByField = new StringField("", 0);
			fieldName = "*";
			td = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {what.toString() + " (" + fieldName + ")"});
//			td = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {null});
		} else {
    		groupByField = tup.getField(gbfield);
    		fieldName = tup.getTupleDesc().getFieldName(gbfield);
    		td = new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE}, new String[] {fieldName, what.toString() + " (" + fieldName + ")"});
		}
		IntField aggField = (IntField) tup.getField(afield);
		int aggFieldVal = aggField.getValue();
		Pair<Integer, Integer> curr = null;
		
		switch (what) {
		case MIN:
			if (!aggregations.containsKey(groupByField))
				aggregations.put(groupByField, new Pair<Integer, Integer>(Integer.MAX_VALUE, null));
			curr = aggregations.get(groupByField);
			if (aggFieldVal < curr.get1()) {
				curr.set1(aggFieldVal);
				aggregations.put(groupByField, curr);
			}
			break;
			
		case MAX:
			if (!aggregations.containsKey(groupByField))
				aggregations.put(groupByField, new Pair<Integer, Integer>(Integer.MIN_VALUE, null));
			curr = aggregations.get(groupByField);
			if (aggFieldVal > curr.get1()) {
				curr.set1(aggFieldVal);
				aggregations.put(groupByField, curr);
			}
			break;
			
		case AVG:
			if (!aggregations.containsKey(groupByField))
				aggregations.put(groupByField, new Pair<Integer, Integer>(0, 0));
			curr = aggregations.get(groupByField);
			curr.set1(curr.get1()+aggFieldVal);
			curr.set2(curr.get2()+1);
			aggregations.put(groupByField, curr);
			break;
			
		case SUM:
			if (!aggregations.containsKey(groupByField))
				aggregations.put(groupByField, new Pair<Integer, Integer>(0, null));
			curr = aggregations.get(groupByField);
			curr.set1(curr.get1()+aggFieldVal);
			aggregations.put(groupByField, curr);
			break;
			
		case COUNT:
			if (!aggregations.containsKey(groupByField))
				aggregations.put(groupByField, new Pair<Integer, Integer>(0, null));
			curr = aggregations.get(groupByField);
			curr.set1(curr.get1()+1);
			aggregations.put(groupByField, curr);
			break;
		default:
			break;
		}
    }

    private Integer getAggVal(Pair<Integer, Integer> vals) {
    	switch (what) {
		case AVG:
			return vals.get1()/vals.get2();
		case COUNT:
		case MAX:
		case MIN:
		case SUM:
			return vals.get1();
		default:
			return -1;
    	}
    		
    }
    
    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    
    public OpIterator iterator() {
    	ArrayList<Tuple> tuples = new ArrayList<>();
    	Tuple t = null;
    	Integer val = null;
    	
    	// create list of tuples
    	for (Map.Entry<Field, Pair<Integer, Integer>> entry : aggregations.entrySet()) {
    		val = getAggVal(entry.getValue());
    		t = new Tuple(td);
    		
    		// NO GROUPING
    		if (gbfield == NO_GROUPING) {
    			t.setField(0, new IntField(val));
    		} else { // GROUPING
    			t.setField(0, entry.getKey());
    			t.setField(1, new IntField(val));
    		}
    		tuples.add(t);
    	}
    	return new TupleIterator(td, tuples);
    }

}
