package cis5550.kvs;

import static cis5550.webserver.Server.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URLDecoder;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import cis5550.kvs.KVSClient.WorkerEntry;

public class Worker extends cis5550.generic.Worker {
    protected static final ConcurrentHashMap<String, ConcurrentHashMap<String, RowVersioning>> tableMap = new ConcurrentHashMap<>();

    public static void main(String[] args) throws InterruptedException {
        validateArguments(args);

        int workerPort = Integer.parseInt(args[0]);
        String workerStorageDirectory = args[1];
        String coordinatorHost = args[2];

        port(workerPort);
        startPingThread(workerPort, workerStorageDirectory, coordinatorHost);
        tableList(tableMap, workerStorageDirectory);
        basicOps(tableMap, workerStorageDirectory);
        final KVSClient kvsClient = new KVSClient(coordinatorHost);
        // startWorkerDownloadThread(kvsClient);
        configureEndpoints(kvsClient, workerStorageDirectory);
        // replicaMaintenance(kvsClient, workerPort, workerStorageDirectory);
    }

    private static void replicaMaintenance(KVSClient kvsClient, int currentWorkerPort, String workerStorageDirectory) {
        new Thread(() -> {  
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    synchronized(kvsClient.workers) {
                        kvsClient.downloadWorkers();
                    }
                    // System.out.println("Starting replica maintenance");
                    String currentWorkerAddr = null;
                    String currentWorkerId = null;
                    for (WorkerEntry worker : kvsClient.workers) {
                        int workerPort = -1;
                        try {
                            workerPort = Integer.parseInt(worker.address.split(":")[1]); //get Port
                        } catch (Exception e) {
                            e.printStackTrace();
                            continue;
                        }
                        if (workerPort != -1 && workerPort == currentWorkerPort){
                            currentWorkerAddr = worker.address;
                            currentWorkerId = worker.id;
                            PersistentTables.streamDataHashFromNextTwoWorkers(currentWorkerAddr, currentWorkerId, kvsClient, workerStorageDirectory);
                            break;
                        }
                    }
                    // System.out.println("-----------");
                } catch (NoSuchAlgorithmException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                try {
                    Thread.sleep(30000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.err.println("Ping thread interrupted");
                    return;
                }
            }
        }).start();
    }

    private static void validateArguments(String[] args) {
        if (args.length != 3) {
            System.err.println("Usage: java cis5550.kvs.Worker <Port of worker> <storage directory> <IP:Port of coordinator>");
            System.exit(1);
        }
    }

    private static void configureEndpoints(KVSClient kvsClient, String workerStorageDirectory) {
        putData(kvsClient, workerStorageDirectory);
        putDataInBatch(kvsClient, workerStorageDirectory);
        getData(workerStorageDirectory);
        getDataFromWholeRow(workerStorageDirectory);
        streamDataFromTable(workerStorageDirectory);
        putRow(kvsClient, workerStorageDirectory);
    }

    private static void putData(KVSClient kvsClient, String workerStorageDirectory) {
        put("/data/:table/:row/:column", (request, response) -> {
            try {
                String tableName = request.params("table");
                String rowKey = URLDecoder.decode(request.params("row"), "UTF-8");
                String column = URLDecoder.decode(request.params("column"), "UTF-8");
                byte[] requestData = request.bodyAsBytes();

                String ifColumn = request.queryParams("ifcolumn");
                String equalsQueryParam = request.queryParams("equals");

                String forwardPut = request.queryParams("forwarded");
                boolean isForwarded = forwardPut == null ? true : true;

                if (tableName.startsWith("pt-")) {
                    PersistentTables.writeRowToPersistentTable(tableName, rowKey, column, requestData, workerStorageDirectory, kvsClient, isForwarded);
                } else {
                    InMemoryTables.writeRowToInMemoryTable(tableMap, tableName, rowKey, column, requestData, ifColumn, equalsQueryParam, response);
                }
                return "OK";
            } catch (Exception e) {
                e.printStackTrace();
                response.status(500, "Internal Server Error");
                return "FAIL";
            }
        });
    }

    private static void putDataInBatch(KVSClient kvsClient, String workerStorageDirectory) {
        put("/data/batched/:table", (request, response) -> {
            try {
                String tableName = request.params("table");
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(request.bodyAsBytes());
                
                List<Row> rows = new ArrayList<Row>();
                while (byteArrayInputStream.available() > 0) {
                    rows.add(Row.readFrom(byteArrayInputStream));
                }

                if (tableName.startsWith("pt-")) {
                    PersistentTables.writeMultipleRowsToPersistentTable(tableName, rows, workerStorageDirectory, kvsClient);
                } else {
                    InMemoryTables.writeMultipleRowsToInMemoryTable(tableMap, tableName, rows, response);
                }
                return "OK";
            } catch (Exception e) {
                e.printStackTrace();
                response.status(500, "Internal Server Error");
                return "FAIL";
            }
        });
    }

    private static void putRow(KVSClient kvsClient, String workerStorageDirectory)
    {
        put("/data/:table", (request, response) -> {
            try {
                String tableName = request.params("table");
                byte[] rowData = request.bodyAsBytes();

                Row row = Row.readFrom(new ByteArrayInputStream(rowData));

                String forwardPut = request.queryParams("forwarded");
                boolean isForwarded = forwardPut == null ? true : true;

                if (tableName.startsWith("pt-")) {
                    PersistentTables.writeWholeRowToPersistentTable(tableName, row, workerStorageDirectory, kvsClient, isForwarded);
                } else {
                    // InMemoryTables.writeWholeRowToInMemoryTable(tableMap, tableName, row, ifColumn, equalsQueryParam, response);
                }
                return "OK";
            } catch (Exception e) {
                e.printStackTrace();
                response.status(500, "Internal Server Error");
                return "FAIL";
            }
        });
    }

    private static void getData(String workerStorageDirectory) {
        get("/data/:table/:row/:column", (request, response) -> {
            try {
                String tableName = request.params("table");
                String rowKey = URLDecoder.decode(request.params("row"), "UTF-8");
                String column = URLDecoder.decode(request.params("column"), "UTF-8");
                String versionQuery = request.queryParams("version");
                if (tableName.startsWith("pt-")) {
                    PersistentTables.readFromPersistentTable(tableName, rowKey, column, response, workerStorageDirectory);
                } else {
                    InMemoryTables.readFromInMemoryTableTable(tableName, rowKey, column, versionQuery, response);
                }
                return null;
            } catch (Exception e) {
                e.printStackTrace();
                response.status(500, "Internal Server Error");
                return "FAIL";
            }
        });
    }

    private static void getDataFromWholeRow(String workerStorageDirectory) {
        get("/data/:table/:row", (request, response) -> {
            String tableName = request.params("table");
            String rowKey = URLDecoder.decode(request.params("row"), "UTF-8");
            String versionQuery = request.queryParams("version");
            if (tableName.startsWith("pt-")) {
                PersistentTables.readWholeRowFromPersistentTable(tableName, rowKey, response, workerStorageDirectory);
            } else {
                InMemoryTables.readWholeRowFromInMemoryTable(tableName, rowKey, versionQuery, response);
            }
            return null;
        });
    }

    private static void streamDataFromTable(String workerStorageDirectory) {
        get("/data/:table", (request, response) -> {
            String tableName = request.params("table");
            String startRow = request.queryParams("startRow");
            String endRowExclusive = request.queryParams("endRowExclusive");
            
            // Check if the strings are empty and set them to null if they are
            startRow = startRow != null && startRow.isEmpty() ? null : startRow;
            endRowExclusive = endRowExclusive != null && endRowExclusive.isEmpty() ? null : endRowExclusive;

            if (tableName.startsWith("pt-")) {
                PersistentTables.streamDataFromPersistentTable(tableName, response, startRow, endRowExclusive, workerStorageDirectory);
            } else {
                InMemoryTables.streamDataFromInMemoryTable(tableName, startRow, endRowExclusive, response);
            }
            return null;
        });
    }

    private static void startWorkerDownloadThread(KVSClient kvsClient) {
        Thread workerDownloadThread = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    synchronized(kvsClient.workers) {
                        kvsClient.downloadWorkers();
                    }
                    Thread.sleep(5000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
        workerDownloadThread.start();
    }
}
