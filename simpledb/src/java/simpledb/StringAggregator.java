package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    
    private TupleDesc td;
    private HashMap<Field, Integer> aggregations = new HashMap<>();
    
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if (what != Op.COUNT)
        	throw new IllegalArgumentException("Only supports COUNT");
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
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
		
		if (!aggregations.containsKey(groupByField))
			aggregations.put(groupByField, Integer.valueOf(0));
		aggregations.put(groupByField, aggregations.get(groupByField)+1);
    }
    
    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
    	ArrayList<Tuple> tuples = new ArrayList<>();
    	Tuple t = null;
    	Integer val = null;
    	
    	// create list of tuples
    	for (Map.Entry<Field, Integer> entry : aggregations.entrySet()) {
    		val = entry.getValue();
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
