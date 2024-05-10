package cis5550.flame;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Serializer;

public class FlamePairRDDImpl implements FlamePairRDD {
	private String tableName;
	private KVSClient kvsClient;
	private FlameContextImpl flameContextImpl;
	private boolean pairRddExists;

	public FlamePairRDDImpl(String tableName, KVSClient kvsClient, FlameContextImpl flameContextImpl) {
		this.tableName = tableName;
		this.kvsClient = kvsClient;
		this.flameContextImpl = flameContextImpl;
		this.pairRddExists = true;
	}

	public String fetchTableName() {
        return this.tableName;
    }

	@Override
	public List<FlamePair> collect() throws Exception {
		List<FlamePair> result = new ArrayList<>();
		Iterator<Row> iteratorRow = kvsClient.scan(tableName);
		while (iteratorRow.hasNext()) {
			Row row = iteratorRow.next();
			String rowKey = row.key();
			for (String column : row.columns()) {
				String columnValue = row.get(column);
				result.add(new FlamePair(rowKey, columnValue));
			}
		}
		return result;
	}

	@Override
	public void saveAsTable(String tableNameArg) throws Exception {
		kvsClient.rename(tableName, tableNameArg);
        this.tableName = tableNameArg;
	}

	@Override
    public void destroy() throws Exception {
        if (this.pairRddExists) {
            kvsClient.delete(tableName);
            this.pairRddExists = false;
        } else { // Future invocations on delete() should throw exceotions
            throw new NoSuchElementException("Table does not exist!");
        }
    }

	private void throwExceptionIfPairRddNotExists() {
		if (this.pairRddExists == false) {
			throw new NoSuchElementException("Table does not exist!");
		}
	}

	@Override
	public FlameRDD flatMap(PairToStringIterable lambda) {
		throwExceptionIfPairRddNotExists();
		byte[] dataAsBytes = Serializer.objectToByteArray(lambda);
		String flatMapTableName = flameContextImpl.invokeOperation("/pairRdd/flatMap", dataAsBytes, FlameRDD.class, tableName, null);

		FlameRDDImpl flatMapTableNameRdd = new FlameRDDImpl(flatMapTableName, kvsClient, flameContextImpl);
		return flatMapTableNameRdd;
	}

	@Override
	public FlamePairRDD foldByKey(String zeroElement, TwoStringsToString lambda) {
		throwExceptionIfPairRddNotExists();
		byte[] dataAsBytes = Serializer.objectToByteArray(lambda);
		zeroElement = URLEncoder.encode(zeroElement, StandardCharsets.UTF_8);
		String foldByKeyTableName = flameContextImpl.invokeOperation("/pairRdd/foldByKey", dataAsBytes, FlamePairRDD.class, tableName, zeroElement);
		FlamePairRDDImpl foldByKeyRdd = new FlamePairRDDImpl(foldByKeyTableName, kvsClient, flameContextImpl);
		return foldByKeyRdd;
	}

	@Override
	public FlamePairRDD flatMapToPair(PairToPairIterable lambda) {
		throwExceptionIfPairRddNotExists();
		byte[] dataAsBytes = Serializer.objectToByteArray(lambda);
		String flatMapTableName = flameContextImpl.invokeOperation("/pairRdd/flatMapToPair", dataAsBytes, FlamePairRDD.class, tableName, null);

		FlamePairRDD flatMapTableNameRdd = new FlamePairRDDImpl(flatMapTableName, kvsClient, flameContextImpl);
		return flatMapTableNameRdd;
	}

	@Override
	public FlamePairRDD join(FlamePairRDD other) {
		throwExceptionIfPairRddNotExists();
		String otherTable = ((FlamePairRDDImpl)other).fetchTableName();
		String joinedTableName = flameContextImpl.invokeOperation("/pairRdd/join", otherTable.getBytes(), FlamePairRDD.class, tableName, null);

		FlamePairRDD joinedTable = new FlamePairRDDImpl(joinedTableName, kvsClient, flameContextImpl);
		return joinedTable;
	}

	@Override
	public FlamePairRDD cogroup(FlamePairRDD other) {
		throwExceptionIfPairRddNotExists();
		String otherTable = ((FlamePairRDDImpl)other).fetchTableName();
		String coGroupTableName = flameContextImpl.invokeOperation("/pairRdd/coGroup", otherTable.getBytes(), FlamePairRDD.class, tableName, null);

		FlamePairRDD coGroupTable = new FlamePairRDDImpl(coGroupTableName, kvsClient, flameContextImpl);
		return coGroupTable;
	}
}
