package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.IntField;
import simpledb.storage.StringField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op op;
    private TupleDesc td;
    private final Type[] types;
    private final HashMap<Integer, Tuple> tuples;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;
        this.types = new Type[2];
        this.tuples = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Tuple tuple;
        IntField groupField = (IntField) tup.getField(gbfield);
        types[0] = gbfieldtype;
        types[1] = Type.INT_TYPE;
        td = new TupleDesc(types);
        int key = groupField.hashCode();

        if (op == Op.COUNT) {
            if (tuples.containsKey(key)) {
                tuple = tuples.get(key);
                int val = ((IntField) tuple.getField(1)).getValue();
                tuple.setField(1, new IntField(val + 1));
            } else {
                tuple = new Tuple(td);
                tuple.setField(0, tup.getField(0));
                tuple.setField(1, new IntField(1));
                tuples.put(key, tuple);
            }
        }
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
