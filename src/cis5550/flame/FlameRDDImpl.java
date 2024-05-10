package cis5550.flame;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Vector;

import cis5550.flame.FlamePairRDD.TwoStringsToString;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

public class FlameRDDImpl implements FlameRDD {
    private String tableName;
    private KVSClient kvsClient;
    private FlameContextImpl flameContextImpl;
    private boolean rddExists;

    public FlameRDDImpl(String tableName, KVSClient kvsClient, FlameContextImpl flameContextImpl) {
        this.tableName = tableName;
        this.kvsClient = kvsClient;
        this.flameContextImpl = flameContextImpl;
        this.rddExists = true;
    }

    public String fetchTableName() {
        return this.tableName;
    }

    @Override
    public List<String> collect() throws Exception {
        List<String> result = new ArrayList<>();
        Iterator<Row> iteratorRow = kvsClient.scan(tableName);
        while (iteratorRow.hasNext()) {
            Row row = iteratorRow.next();
            String value = row.get("value");
            if (value != null) {
                result.add(value);
            }
        }
        return result;
    }

    @Override
    public int count() throws Exception {
        return kvsClient.count(this.tableName);
    }

    @Override
    public void saveAsTable(String tableNameArg) throws Exception {
        kvsClient.rename(tableName, tableNameArg);
        this.tableName = tableNameArg;
    }

    @Override
    public void destroy() throws Exception {
        if (this.rddExists) {
            kvsClient.delete(tableName);
            this.rddExists = false;
        } else { // Future invocations on delete() should throw exceotions
            throw new NoSuchElementException("Table does not exist!");
        }
    }

    private void throwExceptionIfRddNotExists() {
		if (this.rddExists == false) {
			throw new NoSuchElementException("Table does not exist!");
		}
	}

    @Override
    public Vector<String> take(int num) throws Exception {
        if (num < 0) {
            throw new IllegalArgumentException("Number of rows to take must be non-negative.");
        }
        
        Vector<String> rowVector = new Vector<>();
        if (num == 0) {
            return rowVector;
        }
        
        Iterator<Row> rows = kvsClient.scan(tableName);
        while (rows.hasNext() && num > 0) {
            Row row = rows.next();
            rowVector.add(row.get("value"));
            num--;
        }
        return rowVector;
    }

    @Override
    public FlameRDD distinct() {
        throwExceptionIfRddNotExists();
        String distinctTableName = flameContextImpl.invokeOperation("/rdd/distinct", null, FlameRDD.class, tableName, null);
        
        FlameRDDImpl distinctRdd = new FlameRDDImpl(distinctTableName, kvsClient, flameContextImpl);
        return distinctRdd;
    }

    @Override
    public FlameRDD flatMap(StringToIterable lambda) {
        throwExceptionIfRddNotExists();
        byte[] dataAsBytes = Serializer.objectToByteArray(lambda);
        String flatMapTableName = flameContextImpl.invokeOperation("/rdd/flatMap", dataAsBytes, FlameRDD.class, tableName, null);
        
        FlameRDDImpl flatMapRdd = new FlameRDDImpl(flatMapTableName, kvsClient, flameContextImpl);
        return flatMapRdd;
    }

    @Override
    public FlamePairRDD mapToPair(StringToPair lambda) {
        throwExceptionIfRddNotExists();
        byte[] dataAsBytes = Serializer.objectToByteArray(lambda);
        String mapToPairTableName = flameContextImpl.invokeOperation("/rdd/mapToPair", dataAsBytes, FlamePairRDD.class, tableName, null);
        
        FlamePairRDDImpl mapToPairRdd = new FlamePairRDDImpl(mapToPairTableName, kvsClient, flameContextImpl);
        return mapToPairRdd;
    }

    @Override
    public FlameRDD intersection(FlameRDD r) {
        throwExceptionIfRddNotExists();
        String otherTable = ((FlameRDDImpl)r).fetchTableName();
        String firstTable = flameContextImpl.invokeOperation("/rdd/utilities/createRddTable", null, FlameRDD.class, tableName, null);
        String secondTable = flameContextImpl.invokeOperation("/rdd/utilities/createRddTable", null, FlameRDD.class, otherTable, null);

        String intersectionTable = flameContextImpl.invokeOperation("/rdd/intersection-intersect", firstTable.getBytes(), FlameRDD.class, secondTable, null);
        
        FlameRDDImpl intersectionRdd = new FlameRDDImpl(intersectionTable, kvsClient, flameContextImpl);
        return intersectionRdd;
    }

    @Override
    public FlameRDD sample(double f) {
        throwExceptionIfRddNotExists();
        ByteBuffer buffer = ByteBuffer.allocate(Double.BYTES);
        byte[] lambda = buffer.putDouble(f).array();
        String samplingOperationTableName = flameContextImpl.invokeOperation("/rdd/samplingOperation", lambda, FlameRDD.class, tableName, null);

        FlameRDDImpl samplingOperationRdd = new FlameRDDImpl(samplingOperationTableName, kvsClient, flameContextImpl);
        return samplingOperationRdd;
    }

    @Override
    public FlamePairRDD groupBy(StringToString lambda) {
        throwExceptionIfRddNotExists();
        byte[] dataAsBytes = Serializer.objectToByteArray(lambda);
        String groupByTableName = flameContextImpl.invokeOperation("/rdd/groupBy", dataAsBytes, FlamePairRDD.class, tableName, null);

        String multiValuedGroupByTableName = flameContextImpl.invokeOperation("/rdd/groupByMultiValues", null, FlamePairRDD.class, groupByTableName, null);

        FlamePairRDDImpl multiValuedGroupByTableRdd = new FlamePairRDDImpl(multiValuedGroupByTableName, kvsClient, flameContextImpl);
        return multiValuedGroupByTableRdd;
    }

    @Override
    public String fold(String zeroElement, TwoStringsToString lambda) throws FileNotFoundException, IOException {
        throwExceptionIfRddNotExists();
        byte[] dataAsBytes = Serializer.objectToByteArray(lambda);
        zeroElement = URLEncoder.encode(zeroElement, StandardCharsets.UTF_8);
        String foldTableName = flameContextImpl.invokeOperation("/rdd/fold", dataAsBytes, FlameRDD.class, tableName, zeroElement);
        
        Iterator<Row> rowsOfFoldTable = kvsClient.scan(foldTableName);
        String accumulator = zeroElement;
        if (rowsOfFoldTable != null) {
            while (rowsOfFoldTable.hasNext()) {
                Row row = rowsOfFoldTable.next();
                accumulator = lambda.op(accumulator, row.get("value"));
            }
        }
        return accumulator;
    }

    @Override
    public FlamePairRDD flatMapToPair(StringToPairIterable lambda) {
        throwExceptionIfRddNotExists();
        byte[] dataAsBytes = Serializer.objectToByteArray(lambda);
        String flatMapToPairTableName = flameContextImpl.invokeOperation("/rdd/flatMapToPair", dataAsBytes, FlamePairRDD.class, tableName, null);

        FlamePairRDDImpl flatMapToPair = new FlamePairRDDImpl(flatMapToPairTableName, kvsClient, flameContextImpl);
        return flatMapToPair;
    }

    @Override
    public FlameRDD filter(StringToBoolean lambda) {
        throwExceptionIfRddNotExists();
        byte[] dataAsBytes = Serializer.objectToByteArray(lambda);
        String filteredTableName = flameContextImpl.invokeOperation("/rdd/filter", dataAsBytes, FlameRDD.class, tableName, null);

        FlameRDDImpl filteredRdd = new FlameRDDImpl(filteredTableName, kvsClient, flameContextImpl);
        return filteredRdd;
    }

    @Override
    public FlameRDD mapPartitions(IteratorToIterator lambda) {
        throwExceptionIfRddNotExists();
        byte[] dataAsBytes = Serializer.objectToByteArray(lambda);
        String mapPartitionsTableName = flameContextImpl.invokeOperation("/rdd/mapPartitions", dataAsBytes, FlameRDD.class, tableName, null);

        FlameRDDImpl mapPartitionsRdd = new FlameRDDImpl(mapPartitionsTableName, kvsClient, flameContextImpl);
        return mapPartitionsRdd;
    }
}
