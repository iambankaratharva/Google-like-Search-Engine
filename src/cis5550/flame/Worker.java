package cis5550.flame;

import java.util.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.io.*;

import static cis5550.webserver.Server.*;
import cis5550.tools.Hasher;
import cis5550.tools.Serializer;
import cis5550.kvs.*;

class Worker extends cis5550.generic.Worker {

    public static void main(String args[]) {
		if (args.length != 2) {
			System.err.println("Syntax: Worker <port> <coordinatorIP:port>");
			System.exit(1);
		}

		int port = Integer.parseInt(args[0]);
		String server = args[1];
		startPingThread(port, "__flameworker/"+port, server);
		final File myJAR = new File("__worker"+port+"-current.jar");

		port(port);

		post("/useJAR", (request,response) -> {
			FileOutputStream fos = new FileOutputStream(myJAR);
			fos.write(request.bodyAsBytes());
			fos.close();
			return "OK";
		});

		post("/rdd/flatMap", (request, response) -> {
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            if (inputTable == null || outputTable == null) {
                response.status(400, "Bad Request");
                return "Missing required parameters";
            }
            String startKey = request.queryParams("startKey");
            String endKey = request.queryParams("endKey");
            String coordinator = request.queryParams("coordinator");
            startKey = startKey.equals("null") ? null : startKey;
            endKey = endKey.equals("null") ? null : endKey;
        
            try {
                FlameRDD.StringToIterable function = (FlameRDD.StringToIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        
                KVSClient kvsClient = new KVSClient(coordinator);
                Iterator<Row> rows = kvsClient.scan(inputTable, startKey, endKey);
                while (rows.hasNext()) {
                    Row row = rows.next();
                    String rowvalue = row.get("value");
                    if (rowvalue == null) {
                    } else {
                        Iterable<String> results = function.op(rowvalue);
                        if (results != null) {
                            kvsClient.putBatchRDD(outputTable, results);
                            // for (String result : results) {
                            //     String uniqueKey = Hasher.hash(UUID.randomUUID().toString().replace("-", ""));
                            //     kvsClient.put(outputTable, uniqueKey, "value", result);
                            // }
                        }
                    }
                }
            } catch (Exception e) {
                response.status(500, "Internal Server Error");
                e.printStackTrace();
                // throw e;
            }
            return "OK";
        });
        
        post("/rdd/flatMapToPair", (request, response) -> {
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            if (inputTable == null || outputTable == null) {
                response.status(400, "Bad Request");
                return "Missing required parameters";
            }
            String startKey = request.queryParams("startKey");
            String endKey = request.queryParams("endKey");
            String coordinator = request.queryParams("coordinator");
            startKey = startKey.equals("null") ? null : startKey;
            endKey = endKey.equals("null") ? null : endKey;
        
            try {
                FlameRDD.StringToPairIterable function = (FlameRDD.StringToPairIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        
                KVSClient kvsClient = new KVSClient(coordinator);
                Iterator<Row> rows = kvsClient.scan(inputTable, startKey, endKey);
                while (rows.hasNext()) {
                    Row row = rows.next();
                    Iterable <FlamePair> results = function.op(row.get("value"));
                    if (results == null) {
                        continue;
                    }
                    for (FlamePair result : results) {
                        String uniqueKey = Hasher.hash(UUID.randomUUID().toString().replace("-", ""));
                        kvsClient.put(outputTable, result._1(), uniqueKey, result._2());
                    }
                }
            } catch (Exception e) {
                response.status(500, "Internal Server Error");
                e.printStackTrace();
                // throw e;
            }
            return "OK";
        });

        post("/pairRdd/flatMap", (request, response) -> {
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            if (inputTable == null || outputTable == null) {
                response.status(400, "Bad Request");
                return "Missing required parameters";
            }
            String startKey = request.queryParams("startKey");
            String endKey = request.queryParams("endKey");
            String coordinator = request.queryParams("coordinator");
            startKey = startKey.equals("null") ? null : startKey;
            endKey = endKey.equals("null") ? null : endKey;
        
            try {
                FlamePairRDD.PairToStringIterable function = (FlamePairRDD.PairToStringIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        
                KVSClient kvsClient = new KVSClient(coordinator);
                Iterator<Row> rows = kvsClient.scan(inputTable, startKey, endKey);
                while (rows.hasNext()) {
                    Row row = rows.next();
                    for (String column : row.columns()) {
                        Iterable<String> results = function.op(new FlamePair(row.key(), row.get(column)));
                        if (results != null) {
                            for (String result : results) {
                                String uniqueKey = Hasher.hash(UUID.randomUUID().toString().replace("-", ""));
                                kvsClient.put(outputTable, uniqueKey, "value", result);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                response.status(500, "Internal Server Error");
                e.printStackTrace();
                // throw e;
            }
            return "OK";
        });

        post("/pairRdd/flatMapToPair", (request, response) -> {
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            if (inputTable == null || outputTable == null) {
                response.status(400, "Bad Request");
                return "Missing required parameters";
            }
            String startKey = request.queryParams("startKey");
            String endKey = request.queryParams("endKey");
            String coordinator = request.queryParams("coordinator");
            startKey = startKey.equals("null") ? null : startKey;
            endKey = endKey.equals("null") ? null : endKey;
        
            try {
                FlamePairRDD.PairToPairIterable function = (FlamePairRDD.PairToPairIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
        
                KVSClient kvsClient = new KVSClient(coordinator);
                Iterator<Row> rows = kvsClient.scan(inputTable, startKey, endKey);
                while (rows.hasNext()) {
                    Row row = rows.next();
                    for (String column : row.columns()) {
                        Iterable<FlamePair> results = function.op(new FlamePair(row.key(), row.get(column)));
                        if (results != null) {
                            kvsClient.putBatch(outputTable, results);
                            // for (FlamePair result : results) {
                            //     String uniqueKey = Hasher.hash(UUID.randomUUID().toString().replace("-", ""));
                            //     kvsClient.put(outputTable, result._1(), uniqueKey, result._2());
                            // }
                        }
                    }
                }
            } catch (Exception e) {
                response.status(500, "Internal Server Error");
                e.printStackTrace();
                // throw e;
            }
            return "OK";
        });

        post("/rdd/mapToPair", ((request, response) -> {
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            if (inputTable == null || outputTable == null || request.bodyAsBytes() == null) {
                response.status(400, "Bad Request");
                return null;
            }
            String startKey = request.queryParams("startKey");
            String endKey = request.queryParams("endKey");
            String coordinator = request.queryParams("coordinator");
            startKey = startKey.equals("null") ? null : startKey;
            endKey = endKey.equals("null") ? null : endKey;


            try {
                FlameRDD.StringToPair function = (FlameRDD.StringToPair) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
                if (function == null) {
                    response.status(400, "Bad Request");
                    return null;
                }
                
                KVSClient kvsClient = new KVSClient(coordinator);
                Iterator<Row> rows = kvsClient.scan(inputTable, startKey, endKey);
                while (rows.hasNext()) {
                    Row row = rows.next();
                    for(String column : row.columns()) {
                        FlamePair flamePair = function.op(row.get(column));
                        if (flamePair == null) {
                            continue;
                        }
                        kvsClient.put(outputTable, flamePair.a, row.key(), flamePair.b);
                    }
                }
            } catch (Exception e) {
                response.status(500, "Internal Server Error");
                e.printStackTrace();
                // throw e;
            }
            return "OK";
        }));

        post("/pairRdd/foldByKey", ((request, response) -> {
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String zeroElement = request.queryParams("argument");
            if (inputTable == null || outputTable == null) {
                response.status(400, "Bad Request");
                return "Missing required parameters";
            }
            if (zeroElement == null) {
                zeroElement = "";
            }
            String startKey = request.queryParams("startKey");
            String endKey = request.queryParams("endKey");
            String coordinator = request.queryParams("coordinator");
            startKey = startKey.equals("null") ? null : startKey;
            endKey = endKey.equals("null") ? null : endKey;

            try {
                FlamePairRDD.TwoStringsToString function = (FlamePairRDD.TwoStringsToString) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
                
                KVSClient kvsClient = new KVSClient(coordinator);
                Iterator<Row> rows = kvsClient.scan(inputTable, startKey, endKey);
                while (rows.hasNext()) {
                    String accumulator = URLDecoder.decode(zeroElement, "UTF-8");
                    Row row = rows.next();
                    for (String column : row.columns()) {
                        if (row.get(column) != null) accumulator = function.op(accumulator, row.get(column));
                    }
                    kvsClient.put(outputTable, row.key(), "value", accumulator.toString());
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return "OK";
        }));

        post("/rdd/intersection-intersect", ((request, response) -> {
            String firstTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            if (firstTable == null || outputTable == null) {
                response.status(400, "Bad Request");
                return "Missing required parameters";
            }
            String startKey = request.queryParams("startKey");
            String endKey = request.queryParams("endKey");
            String coordinator = request.queryParams("coordinator");
            startKey = startKey.equals("null") ? null : startKey;
            endKey = endKey.equals("null") ? null : endKey;

            try {
                KVSClient kvsClient = new KVSClient(coordinator);

                byte[] lambda = request.bodyAsBytes();
                String secondTable = new String(lambda);

                Iterator<Row> rowsOfFirstTable = null;
                rowsOfFirstTable = kvsClient.scan(firstTable, startKey, endKey);

                Iterator<Row> rowsOfSecondTable = null;
                rowsOfSecondTable = kvsClient.scan(secondTable, startKey, endKey);

                // Fetch keys from rowsOfFirstTable
                Set<String> table1Keys = new HashSet<>();
                while (rowsOfFirstTable.hasNext()) {
                    Row row = rowsOfFirstTable.next();
                    table1Keys.add(row.get("value"));
                }

                // Fetch keys from rowsOfSecondTable that are also in rowsOfFirstTable
                Set<String> commonKeys = new HashSet<>();
                while (rowsOfSecondTable.hasNext()) {
                    Row row = rowsOfSecondTable.next();
                    if (table1Keys.contains(row.get("value"))) {
                        commonKeys.add(row.get("value"));
                    }
                }

                // Add common keys of both tables into new table which will be our intersection table
                for (String commonKey : commonKeys) {
                    String uniqueKey = Hasher.hash(UUID.randomUUID().toString().replace("-", ""));
                    kvsClient.put(outputTable, uniqueKey, "value", commonKey);
                }
            } catch (Exception e) {
                response.status(500, "Internal Server Error");
                e.printStackTrace();
                // throw e;
            }
            return "OK";
        }));

        post("/rdd/samplingOperation", ((request, response) -> {
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            if (inputTable == null || outputTable == null) {
                response.status(400, "Bad Request");
                return "Missing required parameters";
            }
            String startKey = request.queryParams("startKey");
            String endKey = request.queryParams("endKey");
            String coordinator = request.queryParams("coordinator");
            startKey = startKey.equals("null") ? null : startKey;
            endKey = endKey.equals("null") ? null : endKey;

            try {
                double f = ByteBuffer.wrap(request.bodyAsBytes()).getDouble();
                KVSClient kvsClient = new KVSClient(coordinator);
                Iterator<Row> rows = kvsClient.scan(inputTable, startKey, endKey);

                List<String> sampledResults = new ArrayList<String>();
                if (rows != null) {
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        if (Math.random() < f) {
                            sampledResults.add(row.get("value"));
                        }
                    }
                }
                if (!sampledResults.isEmpty()) {
                    for (String sampledResult : sampledResults) {
                        String uniqueKey = Hasher.hash(UUID.randomUUID().toString().replace("-", ""));
                        kvsClient.put(outputTable, uniqueKey, "value", sampledResult);
                    }
                }
            } catch (Exception e) {
                response.status(500, "Internal Server Error");
                e.printStackTrace();
                // throw e;
            }
            return "OK";
        }));

        post("/rdd/groupBy", ((request, response) -> {
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            if (inputTable == null || outputTable == null) {
                response.status(400, "Bad Request");
                return "Missing required parameters";
            }
            String startKey = request.queryParams("startKey");
            String endKey = request.queryParams("endKey");
            String coordinator = request.queryParams("coordinator");
            startKey = startKey.equals("null") ? null : startKey;
            endKey = endKey.equals("null") ? null : endKey;

            try {
                FlameRDD.StringToString function = (FlameRDD.StringToString) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
                
                KVSClient kvsClient = new KVSClient(coordinator);
                
                Iterator<Row> rows = null;
                rows = kvsClient.scan(inputTable, startKey, endKey);
                if (rows != null) {
                    while (rows.hasNext()) {
                        String uniqueKey = Hasher.hash(UUID.randomUUID().toString().replace("-", ""));
                        Row row = rows.next();
                        kvsClient.put(outputTable, function.op(row.get("value")), uniqueKey, row.get("value"));
                    }
                }
            } catch (Exception e) {
                response.status(500, "Internal Server Error");
                e.printStackTrace();
                // throw e;
            }
            return "OK";
        }));

        post("/rdd/groupByMultiValues", ((request, response) -> {
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            if (inputTable == null || outputTable == null) {
                response.status(400, "Bad Request");
                return "Missing required parameters";
            }
            String startKey = request.queryParams("startKey");
            String endKey = request.queryParams("endKey");
            String coordinator = request.queryParams("coordinator");
            startKey = startKey.equals("null") ? null : startKey;
            endKey = endKey.equals("null") ? null : endKey;

            try {
                KVSClient kvsClient = new KVSClient(coordinator);
                
                Iterator<Row> rows = null;
                rows = kvsClient.scan(inputTable, startKey, endKey);
                if (rows != null) {
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        StringBuilder rowValueBuilder = new StringBuilder();
                        for (String column : row.columns()) {
                            if (rowValueBuilder.length() > 0) {
                                rowValueBuilder.append(",");
                            }
                            rowValueBuilder.append(row.get(column));
                        }
                        String rowValue = rowValueBuilder.toString();

                        kvsClient.put(outputTable, row.key(), "aggregatedColumn", rowValue);
                    }
                }
            } catch (Exception e) {
                response.status(500, "Internal Server Error");
                e.printStackTrace();
                // throw e;
            }
            return "OK";
        }));

        post("/rdd/utilities/createRddTable", ((request, response) -> {
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            if (inputTable == null || outputTable == null) {
                response.status(400, "Bad Request");
                return "Missing required parameters";
            }
            String startKey = request.queryParams("startKey");
            String endKey = request.queryParams("endKey");
            String coordinator = request.queryParams("coordinator");
            startKey = startKey.equals("null") ? null : startKey;
            endKey = endKey.equals("null") ? null : endKey;

            try {
                KVSClient kvsClient = new KVSClient(coordinator);
                Iterator<Row> rows = kvsClient.scan(inputTable, startKey, endKey);

                if (rows != null) {
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        String rowValue = URLEncoder.encode(row.get("value"), StandardCharsets.UTF_8);
                        kvsClient.put(outputTable, rowValue, "value", rowValue);
                    }
                }
            } catch (Exception e) {
                response.status(500, "Internal Server Error");
                e.printStackTrace();
                // throw e;
            }
            return "OK";
        }));

        post("/rdd/fromTable", ((request, response) -> {
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            if (inputTable == null || outputTable == null) {
                response.status(400, "Bad Request");
                return "Missing required parameters";
            }
            String startKey = request.queryParams("startKey");
            String endKey = request.queryParams("endKey");
            String coordinator = request.queryParams("coordinator");
            startKey = startKey.equals("null") ? null : startKey;
            endKey = endKey.equals("null") ? null : endKey;

            try {
                KVSClient kvsClient = new KVSClient(coordinator);
                Iterator<Row> rows = kvsClient.scan(inputTable, startKey, endKey);
                FlameContext.RowToString function = (FlameContext.RowToString) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);

                if (rows != null) {
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        String rowValue = function.op(row);
                        if (rowValue != null) {
                            String uniqueKey = Hasher.hash(UUID.randomUUID().toString().replace("-", ""));
                            kvsClient.put(outputTable, uniqueKey, "value", rowValue);
                        }
                    }
                }
            } catch (Exception e) {
                response.status(500, "Internal Server Error");
                e.printStackTrace();
                // throw e;
            }
            return "OK";
        }));

        post("/rdd/distinct", ((request, response) -> {
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            if (inputTable == null || outputTable == null) {
                response.status(400, "Bad Request");
                return "Missing required parameters";
            }
            String startKey = request.queryParams("startKey");
            String endKey = request.queryParams("endKey");
            String coordinator = request.queryParams("coordinator");
            startKey = startKey.equals("null") ? null : startKey;
            endKey = endKey.equals("null") ? null : endKey;

            try {
                KVSClient kvsClient = new KVSClient(coordinator);
                Iterator<Row> rows = kvsClient.scan(inputTable, startKey, endKey);

                if (rows != null) {
                    while (rows.hasNext()) {
                        Row row = rows.next();
                        String value = row.get("value");
                        kvsClient.put(outputTable, value, "value", value);
                    }
                }
            } catch (Exception e) {
                response.status(500, "Internal Server Error");
                e.printStackTrace();
                // throw e;
            }
            return "OK";
        }));

        post("/pairRdd/join", ((request, response) -> {
            String firstTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            if (firstTable == null || outputTable == null) {
                response.status(400, "Bad Request");
                return "Missing required parameters";
            }
            String startKey = request.queryParams("startKey");
            String endKey = request.queryParams("endKey");
            String coordinator = request.queryParams("coordinator");
            startKey = startKey.equals("null") ? null : startKey;
            endKey = endKey.equals("null") ? null : endKey;

            try {
                KVSClient kvsClient = new KVSClient(coordinator);

                String secondTable = new String(request.bodyAsBytes());
                
                Iterator<Row> rowsOfFirstTable = kvsClient.scan(firstTable, startKey, endKey);
                Iterator<Row> rowsOfSecondTable = kvsClient.scan(secondTable, startKey, endKey);

                // Fetch keys from rowsOfFirstTable
                Set<String> table1Keys = new HashSet<>();
                while (rowsOfFirstTable.hasNext()) {
                    Row row = rowsOfFirstTable.next();
                    table1Keys.add(row.key());
                }

                if (table1Keys.isEmpty()) {
                    return null;
                }

                // Fetch keys from rowsOfSecondTable that are also in rowsOfFirstTable
                while (rowsOfSecondTable.hasNext()) {
                    Row row = rowsOfSecondTable.next();
                    String secondTableRowKey = row.key();
                    if (table1Keys.contains(row.key())) {
                        Set<String> columnsOfTable1 = kvsClient.getRow(firstTable, secondTableRowKey).columns();
                        Set<String> columnsOfTable2 = row.columns();
                        for (String columnOfTable1 : columnsOfTable1) {
                            for (String columnOfTable2 : columnsOfTable2) {
                                String dataOfTable1 = new String (kvsClient.get(firstTable, secondTableRowKey, columnOfTable1));
                                String dataOfTable2 = row.get(columnOfTable2);
                                kvsClient.put(outputTable, secondTableRowKey, (columnOfTable1+columnOfTable2), (dataOfTable1  + "," + dataOfTable2));
                            }
                        }
                    }
                }
            } catch (Exception e) {
                response.status(500, "Internal Server Error");
                e.printStackTrace();
                // throw e;
            }
            return "OK";
        }));

        post("/rdd/fold", ((request, response) -> {
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String zeroElement = request.queryParams("argument");
            if (inputTable == null || outputTable == null || zeroElement == null) {
                response.status(400, "Bad Request");
                return "Missing required parameters";
            }
            zeroElement = URLDecoder.decode(zeroElement, StandardCharsets.UTF_8);
            String startKey = request.queryParams("startKey");
            String endKey = request.queryParams("endKey");
            String coordinator = request.queryParams("coordinator");
            startKey = startKey.equals("null") ? null : startKey;
            endKey = endKey.equals("null") ? null : endKey;

            try {
                String uniqueKey = Hasher.hash(UUID.randomUUID().toString().replace("-", ""));
                FlamePairRDD.TwoStringsToString function = (FlamePairRDD.TwoStringsToString) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
                
                KVSClient kvsClient = new KVSClient(coordinator);
                Iterator<Row> rows = kvsClient.scan(inputTable, startKey, endKey);
                while (rows.hasNext()) {
                    String accumulator = zeroElement;
                    Row row = rows.next();
                    for (String column : row.columns()) {
                        accumulator = function.op(accumulator, row.get(column));
                    }
                    String rowName = uniqueKey + row.key();
                    // Data distribution happens as per row.key(); range-based. For fold we need all values on the same worker.
                    // Hence, add a fixed uniqueKey prefix to each row.key().
                    kvsClient.put(outputTable, rowName, "value", accumulator);
                }
            } catch (Exception e) {
                response.status(500, "Internal Server Error");
                e.printStackTrace();
                // throw e;
            }
            return "OK";
        }));

        post("/rdd/filter", ((request, response) -> {
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            if (inputTable == null || outputTable == null) {
                response.status(400, "Bad Request");
                return "Missing required parameters";
            }
            String startKey = request.queryParams("startKey");
            String endKey = request.queryParams("endKey");
            String coordinator = request.queryParams("coordinator");
            startKey = startKey.equals("null") ? null : startKey;
            endKey = endKey.equals("null") ? null : endKey;

            try {
                FlameRDD.StringToBoolean function = (FlameRDD.StringToBoolean) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
                
                KVSClient kvsClient = new KVSClient(coordinator);
                Iterator<Row> rows = kvsClient.scan(inputTable, startKey, endKey);
                while (rows.hasNext()) {
                    Row row = rows.next();
                    String rowValue = row.get("value");
                    if (function.op(rowValue)) {
                        String uniqueKey = Hasher.hash(UUID.randomUUID().toString().replace("-", ""));
                        kvsClient.put(outputTable, uniqueKey, "value", rowValue);
                    }
                }
            } catch (Exception e) {
                response.status(500, "Internal Server Error");
                e.printStackTrace();
                // throw e;
            }
            return "OK";
        }));

        post("/rdd/mapPartitions", ((request, response) -> {
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            if (inputTable == null || outputTable == null) {
                response.status(400, "Bad Request");
                return "Missing required parameters";
            }
            String startKey = request.queryParams("startKey");
            String endKey = request.queryParams("endKey");
            String coordinator = request.queryParams("coordinator");
            startKey = startKey.equals("null") ? null : startKey;
            endKey = endKey.equals("null") ? null : endKey;

            try {
                KVSClient kvsClient = new KVSClient(coordinator);
                Iterator<Row> rows = kvsClient.scan(inputTable, startKey, endKey);

                ArrayList<String> rowValues = new ArrayList<>();
                while (rows.hasNext()) {
                    Row row = rows.next();
                    rowValues.add(row.get("value"));
                }

                FlameRDD.IteratorToIterator function = (FlameRDD.IteratorToIterator) Serializer.byteArrayToObject(request.bodyAsBytes(), myJAR);
                Iterator<String> transformedRowValues = function.op(rowValues.iterator());
                
                if (transformedRowValues != null) {
                    while (transformedRowValues.hasNext()) {
                        String transformedRowValue = transformedRowValues.next();
                        String uniqueKey = Hasher.hash(UUID.randomUUID().toString().replace("-", ""));
                        kvsClient.put(outputTable, uniqueKey, "value", transformedRowValue);
                    }
                }
            } catch (Exception e) {
                response.status(500, "Internal Server Error");
                e.printStackTrace();
                // throw e;
            }
            return "OK";
        }));

        post("/pairRdd/coGroup", ((request, response) -> {
            String firstTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            if (firstTable == null || outputTable == null) {
                response.status(400, "Bad Request");
                return "Missing required parameters";
            }
            String startKey = request.queryParams("startKey");
            String endKey = request.queryParams("endKey");
            String coordinator = request.queryParams("coordinator");
            startKey = startKey.equals("null") ? null : startKey;
            endKey = endKey.equals("null") ? null : endKey;
            String secondTable = new String(request.bodyAsBytes());

            try {
                KVSClient kvsClient = new KVSClient(coordinator);
                Iterator<Row> rowsOfFirstTable = kvsClient.scan(firstTable, startKey, endKey);
                Iterator<Row> rowsOfSecondTable = kvsClient.scan(secondTable, startKey, endKey);

                // Fetch keys from rowsOfFirstTable
                Set<String> table1Keys = new HashSet<>();
                while (rowsOfFirstTable.hasNext()) {
                    Row row = rowsOfFirstTable.next();
                    table1Keys.add(row.key());
                }

                while (rowsOfSecondTable.hasNext()) {
                    Row row = rowsOfSecondTable.next();
                    String secondTableRowKey = row.key();
                    if (table1Keys.contains(secondTableRowKey)) { // Keys from rowsOfSecondTable that are also in rowsOfFirstTable
                        Set<String> columnsOfTable1 = kvsClient.getRow(firstTable, secondTableRowKey).columns();
                        Set<String> columnsOfTable2 = row.columns();
                        
                        StringBuilder table1Result = new StringBuilder("[");
                        for (String columnOfTable1 : columnsOfTable1) {
                            table1Result.append(new String(kvsClient.get(firstTable, secondTableRowKey, columnOfTable1))).append(",");
                        }
                        if (!columnsOfTable1.isEmpty()) {
                            table1Result.deleteCharAt(table1Result.length() - 1); // Remove last comma
                        }
                        table1Result.append("]");

                        StringBuilder table2Result = new StringBuilder("[");
                        for (String columnOfTable2 : columnsOfTable2) {
                            table2Result.append(new String(kvsClient.get(secondTable, secondTableRowKey, columnOfTable2))).append(",");
                        }
                        if (!columnsOfTable2.isEmpty()) {
                            table2Result.deleteCharAt(table2Result.length() - 1); // Remove last comma
                        }
                        table2Result.append("]");
                        
                        String uniqueKey = Hasher.hash(UUID.randomUUID().toString().replace("-", ""));
                        StringBuilder combinedResult = new StringBuilder(table1Result.toString()).append(",").append(table2Result.toString());
                        kvsClient.put(outputTable, secondTableRowKey, uniqueKey, combinedResult.toString());
                    } else { // Keys from rowsOfSecondTable that are not in rowsOfFirstTable
                        Set<String> columnsOfTable2 = row.columns();
                        StringBuilder table2Result = new StringBuilder("[");
                        for (String columnOfTable2 : columnsOfTable2) {
                            table2Result.append(new String(kvsClient.get(secondTable, secondTableRowKey, columnOfTable2))).append(",");
                        }
                        if (!columnsOfTable2.isEmpty()) {
                            table2Result.deleteCharAt(table2Result.length() - 1); // Remove last comma
                        }
                        table2Result.append("]");
                        
                        String uniqueKey = Hasher.hash(UUID.randomUUID().toString().replace("-", ""));
                        kvsClient.put(outputTable, secondTableRowKey, uniqueKey, ("[]," + table2Result.toString()));
                    }
                }

                for (String table1Key : table1Keys) {
                    if (kvsClient.getRow(outputTable, table1Key) == null ) { // Keys from rowsOfFirstTable that are not in rowsOfSecondTable
                        Row row = kvsClient.getRow(firstTable, table1Key);
                        Set<String> columnsOfTable1 = new HashSet<>();
                        if (row != null)
                            columnsOfTable1 = row.columns();
                        StringBuilder table1Result = new StringBuilder("[");
                        for (String columnOfTable1 : columnsOfTable1) {
                            byte[] temp = kvsClient.get(firstTable, table1Key, columnOfTable1);
                            if (temp != null)
                                table1Result.append(new String(temp)).append(",");
                        }
                        if (!columnsOfTable1.isEmpty()) {
                            table1Result.deleteCharAt(table1Result.length() - 1); // Remove last comma
                        }
                        table1Result.append("]");
                        
                        String uniqueKey = Hasher.hash(UUID.randomUUID().toString().replace("-", ""));
                        kvsClient.put(outputTable, table1Key, uniqueKey, (table1Result.toString() + ",[]"));
                    }
                }
            } catch (Exception e) {
                response.status(500, "Internal Server Error");
                e.printStackTrace();
                // throw e;
            }
            return "OK";
        }));

	}
}