package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op op;
    private TupleDesc td;
    private final Type[] types;
    private final HashMap<Integer, Tuple> tuples;
    private final HashMap<Integer, Integer> counts;

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

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;
        this.types = new Type[2];
        this.tuples = new HashMap<>();
        this.counts = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Tuple tuple;
        IntField aggField = (IntField) tup.getField(afield);
        types[0] = gbfieldtype;
        types[1] = aggField.getType();
        td = new TupleDesc(types);
        int key = tup.getField(gbfield).hashCode();
        int val = aggField.getValue();

        if (op == Op.SUM) {
            if (tuples.containsKey(key)) {
                tuple = tuples.get(key);
                aggField = new IntField(val + ((IntField) tuple.getField(1)).getValue());
                tuple.setField(1, aggField);
            } else {
                tuples.put(key, tup);
            }
        }

        if (op == Op.AVG) {
            if (tuples.containsKey(key)) {
                int count = counts.get(key);
                counts.replace(key, count, count + 1);
                tuple = tuples.get(key);
                aggField = new IntField((val + ((IntField) tuple.getField(1)).getValue() * count) / (count + 1));
                tuple.setField(1, aggField);
            } else {
                tuples.put(key, tup);
                counts.put(key, 1);
            }
        }

        if (op == Op.MIN) {
            if (tuples.containsKey(key)) {
                tuple = tuples.get(key);
                if (val < ((IntField) tuple.getField(1)).getValue()) {
                    aggField = new IntField(val);
                    tuple.setField(1, aggField);
                }
            } else {
                tuples.put(key, tup);
            }
        }

        if (op == Op.MAX) {
            if (tuples.containsKey(key)) {
                tuple = tuples.get(key);
                if (val > ((IntField) tuple.getField(1)).getValue()) {
                    aggField = new IntField(val);
                    tuple.setField(1, aggField);
                }
            } else {
                tuples.put(key, tup);
            }
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
        // some code goes here
        return new OpIterator() {
            private Iterator<Tuple> it;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                it = tuples.values().iterator();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                return it.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                return it.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return td;
            }

            @Override
            public void close() {
                it = null;
            }
        };
    }

}
