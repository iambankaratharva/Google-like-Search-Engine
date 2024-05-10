package cis5550.kvs;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import cis5550.webserver.Response;

public class InMemoryTables {

    protected static boolean writeRowToInMemoryTable(ConcurrentHashMap<String, ConcurrentHashMap<String, RowVersioning>> tableMap, String tableName, String rowKey, String column, byte[] data, String ifColumn, String equalsQueryParam, Response response) {
        RowVersioning rowVersioning = tableMap.computeIfAbsent(tableName, k -> new ConcurrentHashMap<>()).computeIfAbsent(rowKey, k -> new RowVersioning());
    
        if (ifColumn != null && equalsQueryParam != null && !rowVersioning.checkForConditionalPut(ifColumn, equalsQueryParam)) {
            // Conditional PUT will fail
            return false;
        }
    
        Row row = rowVersioning.getCurrentVersionOfRow() > 0 ? rowVersioning.getLatestVersionOfRow().clone() : new Row(rowKey);
        row.put(column, data);
        rowVersioning.AddNewRowVersion(row); // Add cloned or new row to the KVS
        
        int rowVersion = rowVersioning.getCurrentVersionOfRow(); // Get current version after update
        response.header("Version", String.valueOf(rowVersion));
        
        return true;
    }

    public static void writeMultipleRowsToInMemoryTable(ConcurrentHashMap<String, ConcurrentHashMap<String, RowVersioning>> tablemap, String tableName, List<Row> rows, Response response) {
        for (Row row : rows) {
            try {
                for (String column : row.columns()) {
                    writeRowToInMemoryTable(tablemap, tableName, row.key(), column, row.getBytes(column), null, null, response);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    protected static void readFromInMemoryTableTable(String tableName, String rowKey, String column, String versionQuery, Response response) {
        RowVersioning rowVersion = null;
        Integer version;
        byte[] data = null;
        try {
            if (versionQuery != null) {
                version = Integer.parseInt(versionQuery);
            } else {
                version = null;
            }
            rowVersion = Worker.tableMap.get(tableName).get(rowKey);
            data = rowVersion.getColumnData(column, version);
            if (data == null) {
                response.status(404, "Not Found");
                return;
            }
            response.bodyAsBytes(data);
        } catch (Exception e) {
            response.status(404, "Not Found");
            return;
        }
        if (version == null) { // return latest version
            response.header("Version", String.valueOf(rowVersion.getCurrentVersionOfRow()));
        } else {
            response.header("Version", String.valueOf(version));
        }
        return;
    }

    protected static void readWholeRowFromInMemoryTable(String tableName, String rowKey, String versionQuery, Response response) {
        RowVersioning rowVersion = null;
        Row row = null;
        Integer version;
        try {
            rowVersion = Worker.tableMap.get(tableName).get(rowKey);
            if (versionQuery != null) {
                version = Integer.parseInt(versionQuery);
                row = rowVersion.getVersionOfRow(version);
            } else {
                version = null;
                row = rowVersion.getLatestVersionOfRow();
            }
            response.bodyAsBytes(row.toByteArray());
        } catch (Exception e) {
            response.status(404, "Not Found");
            return;
        }
        if (version == null) { // return latest version
            response.header("Version", String.valueOf(rowVersion.getCurrentVersionOfRow()));
        } else {
            response.header("Version", String.valueOf(version));
        }
    }

    protected static void streamDataFromInMemoryTable(String tableName, String startRow, String endRowExclusive, Response response) {
        try {
            ConcurrentHashMap<String, RowVersioning> rowVersions = Worker.tableMap.get(tableName);
            if(rowVersions != null) {
                List<Map.Entry<String, RowVersioning>> rowVersionEntries = new ArrayList<>(rowVersions.entrySet());
                rowVersionEntries.sort(Map.Entry.comparingByKey());
                response.header("content-type", "text/plain");
                for (Map.Entry<String, RowVersioning> entry : rowVersionEntries) {
                    if (cis5550.generic.Worker.isRowInRange(entry.getKey(), startRow, endRowExclusive)) {
                        try {
                            byte[] latestRowData = entry.getValue().getLatestVersionOfRow().toByteArray();
                            response.write(latestRowData);
                            response.write("\n".getBytes());
                        } catch (Exception e) {
                            System.err.println("error in response.write()");
                            continue;
                        }
                    }
                }
                response.write("\n".getBytes());
            } else {
                response.status(404, "Not Found");
            }
            
        } catch (Exception e) {
            response.status(404, "Not Found");
            e.printStackTrace();
        }
    }
}
