package cis5550.generic;

import static cis5550.tools.HTTP.*;
import static cis5550.webserver.Server.*;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import cis5550.kvs.Row;
import cis5550.kvs.RowVersioning;
import cis5550.tools.KeyEncoder;
public class Worker {
    private static final long PING_INTERVAL = 5 * 1000; // 5 seconds in milliseconds

    protected static void startPingThread(int workerPort, String workerStorageDirectory, String coordinatorHost) {
        // System.out.println("Ping the coordinator for worker port: " + workerPort);
        new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                String workerId = generateRandomString();
                try {
                    Path workerIdFilePath = Paths.get(workerStorageDirectory, "id");
                    if (Files.exists(workerIdFilePath)) {
                        workerId = Files.readString(workerIdFilePath);
                    } else {
                        if (!Files.exists(workerIdFilePath.getParent())) {
                            // If directory exists but doesn't contain id file
                            Files.createDirectories(workerIdFilePath.getParent());
                        }
                        Files.createFile(workerIdFilePath);
                        Files.writeString(workerIdFilePath, workerId);
                    }
                } catch (IOException e) {
                    System.err.println("Error handling worker ID file: " + e.getMessage());
                    continue;
                }
                String url = constructURL(coordinatorHost, workerId, workerPort);
                try {
                    doRequest("GET", url, null);
                    // System.out.println("Triggered HTTP Request for: " + url);
                } catch (IOException e) {
                    System.err.println("Error sending HTTP request: " + e.getMessage());
                    e.printStackTrace();
                }
                try {
                    Thread.sleep(PING_INTERVAL);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    System.err.println("Ping thread interrupted");
                    return;
                }
            }
        }).start();
    }
    
    private static String constructURL(String host, String workerId, int port) {
        return "http://" + host + "/ping?id=" + workerId + "&port=" + port;
    }

    private static String generateRandomString() {
        int length = 5;
        String characters = "abcdefghijklmnopqrstuvwxyz";
        Random random = new Random();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(characters.charAt(random.nextInt(characters.length())));
        }
        return sb.toString();
    }

    protected static void tableList(ConcurrentHashMap<String, ConcurrentHashMap<String, RowVersioning>> tableMap, String workerStorageDirectory) {
        get("/", (request, response) -> {
            StringBuilder table = new StringBuilder();
            response.type("text/html");
            table.append("<html><body><table><tr><th>Table</th><th>Number of Keys</th></tr>");
            Path tableDirectoryPath = Paths.get(workerStorageDirectory);

            if (Files.exists(tableDirectoryPath) && Files.isDirectory(tableDirectoryPath) && Files.isReadable(tableDirectoryPath)) {
                try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(tableDirectoryPath)) {
                    for (Path tablePath : directoryStream) {
                        String tableNameString = tablePath.getFileName().toString();
                        if (tableNameString.startsWith("pt-")) {
                            AtomicInteger noOfRows = new AtomicInteger(0);
                            Files.walkFileTree(tablePath, new SimpleFileVisitor<Path>() {
                                @Override
                                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                                    noOfRows.incrementAndGet();
                                    return FileVisitResult.CONTINUE;
                                }
                            });
                            table.append("<tr><td><a href='/view/").append(tableNameString).append("'>").append(tableNameString).append("</a></td><td>").append(noOfRows).append("</td></tr>");
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            for (String tableName : tableMap.keySet()) { // in-memory tables
                int numberOfKeys = tableMap.get(tableName).size();
                table.append("<tr><td><a href='/view/").append(tableName).append("'>").append(tableName).append("</a></td><td>").append(numberOfKeys).append("</td></tr>");
            }
            table.append("</table></body></html>");
            return table.toString();
        });

        get("/tables", (request, response) -> {
            Path tableDirectoryPath = Paths.get(workerStorageDirectory);
            response.header("content-type", "text/plain");

            if (Files.exists(tableDirectoryPath) && Files.isDirectory(tableDirectoryPath) && Files.isReadable(tableDirectoryPath)) {
                try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(tableDirectoryPath)) {
                    for (Path tablePath : directoryStream) {
                        String tableNameString = tablePath.getFileName().toString();
                        if (tableNameString.startsWith("pt-")) {
                            response.write(tableNameString.getBytes());
                            response.write("\n".getBytes());
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

            for (String tableName : tableMap.keySet()) { // in-memory tables
                response.write(tableName.getBytes());
                response.write("\n".getBytes());
            }
            response.write("\n".getBytes());
            return null;
        });

        get("/FileNames", (request, response) -> {
            final List<String> fileNames = new ArrayList<>();
            Path workerStoragePath = Paths.get(workerStorageDirectory); // Define your workerStorageDirectory path here
        
            if (Files.exists(workerStoragePath) && Files.isDirectory(workerStoragePath) && Files.isReadable(workerStoragePath)) {
                try {
                    Files.walk(workerStoragePath)
                         .filter(Files::isRegularFile)
                         .filter(file -> !file.getFileName().toString().equals("id")) // don't send id file
                         .filter(file -> !file.getFileName().toString().equals("config")) // don't send config file
                         .forEach(file -> fileNames.add(file.toAbsolutePath().toString()));
                } catch (IOException e) {
                    e.printStackTrace();
                    response.status(500, "INTERNAL SERVER ERROR");
                    return null;
                }
            } else {
                response.status(404, "NOT FOUND");
                return null;
            }
        
            response.status(200, "OK");
            String fileListString = String.join(",", fileNames);
            if (fileListString.isEmpty()) {
            } else {
                // System.out.println("fileListString: " + fileListString);
                response.body(fileListString);
            }
            return fileNames;
        });

        get("/view/:table", (request, response) -> {
            String tableName = request.params("table");
            StringBuilder tableView = new StringBuilder();
            tableView.append("<html><body>");
            tableView.append("<h1>Table Name: ").append(tableName).append("</h1>");
            tableView.append("<table>");
            tableView.append("<tr><th>Row Key</th>");

            if (tableName.startsWith("pt-")) { // persistent tables
                Path tableDirectoryPath = Paths.get(workerStorageDirectory, tableName);
            
                if (Files.exists(tableDirectoryPath) && Files.isDirectory(tableDirectoryPath) && Files.isReadable(tableDirectoryPath)) {
                    final List<Path> listOfFiles = new ArrayList<>();
                    try {
                        // Collect all files from this directory and all subdirectories
                        Files.walkFileTree(tableDirectoryPath, new SimpleFileVisitor<Path>() {
                            @Override
                            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                                listOfFiles.add(file);
                                return FileVisitResult.CONTINUE;
                            }
                        });
                    } catch (IOException e) {
                        e.printStackTrace();
                        response.status(500, "INTERNAL SERVER ERROR");
                    }
            
                    if (listOfFiles != null) {
                        // Before sorting, decode each filename
                        Map<Path, String> decodedPathMap = listOfFiles.stream()
                            .collect(Collectors.toMap(
                                Function.identity(), 
                                path -> KeyEncoder.decode(path.getFileName().toString())));

                        listOfFiles.sort(Comparator.comparing(path -> decodedPathMap.get(path)));

                        TreeSet<String> allColumns = new TreeSet<>();
                        LinkedHashMap<String, Map<String, String>> allRows = new LinkedHashMap<>();
                
                        int pageSize = 10;
                        int pageNumber = 1;
                        String pageParam = request.queryParams("page");
                        if (pageParam != null && !pageParam.isEmpty()) {
                            try {
                                pageNumber = Integer.parseInt(pageParam);
                            } catch (NumberFormatException e) {
                                e.printStackTrace();
                            }
                        }
                        int startRow = (pageNumber - 1) * pageSize;
                        int endRow = startRow + pageSize;
                        int currentRow = 0;
                
                        for (Path filePath : listOfFiles) {
                            if (currentRow >= startRow && currentRow < endRow) { // Only process rows for the current page
                                String content = new String(Files.readAllBytes(filePath), StandardCharsets.UTF_8);
                                String[] parts = content.split(" ");
                                if (parts.length > 1) {
                                    String encodedRowKey = parts[0];
                                    String rowKey = KeyEncoder.decode(encodedRowKey);
                                    Map<String, String> rowData = new HashMap<>();

                                    for (int i = 1; i < parts.length; i += 3) {
                                        if (i + 2 < parts.length) {
                                            String colName = parts[i];
                                            int len = Integer.parseInt(parts[i + 1]);
                                            String data = parts[i + 2].substring(0, Math.min(len, parts[i + 2].length())); // Ensure the data respects the length
                                            allColumns.add(colName);
                                            rowData.put(colName, data);
                                        }
                                    }
                                    allRows.put(rowKey, rowData);
                                }
                            }
                            currentRow++;
                        }

                        for (String column : allColumns) {
                            tableView.append("<th>").append(column).append("</th>");
                        }
                        tableView.append("</tr>");

                        for (Map.Entry<String, Map<String, String>> rowEntry : allRows.entrySet()) {
                            tableView.append("<tr><td>").append(rowEntry.getKey()).append("</td>");
                            Map<String, String> rowData = rowEntry.getValue();
                            for (String column : allColumns) {
                                tableView.append("<td>").append(rowData.getOrDefault(column, "")).append("</td>");
                            }
                            tableView.append("</tr>");
                        }
                        tableView.append("</table>");

                        tableView.append("<div>");
                        if (currentRow > endRow) {
                            tableView.append("<a href=\"/view/").append(tableName).append("?page=").append(pageNumber + 1).append("\">Next</a>");
                        }
                        tableView.append("</div>");
                    }
                }
            } else { // in-memory tables
                ConcurrentHashMap<String, RowVersioning> tableData = tableMap.get(tableName);
        
                if (tableData == null) {
                    response.status(404, "Bad Request");
                    return "Table not found";
                }
            
                // Use TreeSet with a case-insensitive order comparator to sort by keys & columns
                TreeSet<String> columns = new TreeSet<>();
                for (RowVersioning row : tableData.values()) {
                    Row latestRow = row.getLatestVersionOfRow();
                    if (latestRow != null) {
                        columns.addAll(latestRow.columns());
                    }
                }
            
                for (String column : columns) {
                    tableView.append("<th>").append(column).append("</th>");
                }
                tableView.append("</tr>");
            
                // Pagination logic
                int pageSize = 10;
                int pageNumber = 1;
                String pageParam = request.queryParams("page");
                if (pageParam != null && !pageParam.isEmpty()) {
                    try {
                        pageNumber = Integer.parseInt(pageParam);
                    } catch (NumberFormatException e) {
                        e.printStackTrace();
                    }
                }
            
                int startRow = (pageNumber - 1) * pageSize;
                int endRow = startRow + pageSize;
            
                // Display sorted rows for current page
                int rowCount = 0;
                // Sort the entries of the map by keys in a case-sensitive manner
                List<Map.Entry<String, RowVersioning>> sortedEntries = new ArrayList<>(tableData.entrySet());
                sortedEntries.sort(Map.Entry.comparingByKey());
            
                for (Map.Entry<String, RowVersioning> entry : sortedEntries) {
                    if (rowCount >= startRow && rowCount < endRow) {
                        String rowKey = entry.getKey();
                        Row latestRow = entry.getValue().getLatestVersionOfRow();
                        if (latestRow != null) {
                            tableView.append("<tr><td>").append(rowKey).append("</td>");
                            for (String column : columns) { // Columns are already sorted in a case-sensitive manner
                                byte[] value = latestRow.getBytes(column);
                                tableView.append("<td>").append(value != null ? new String(value) : "").append("</td>");
                            }
                            tableView.append("</tr>");
                        }
                    }
                    rowCount++;
                }
                tableView.append("</table>");
            
                tableView.append("<div>");
                if (rowCount > endRow) {
                    tableView.append("<a href=\"/view/").append(tableName).append("?page=").append(pageNumber + 1).append("\">Next</a>");
                }
                tableView.append("</div>");
            }
            tableView.append("</body></html>");
            return tableView.toString();
        });        
    }

    protected static void basicOps(ConcurrentHashMap<String, ConcurrentHashMap<String, RowVersioning>> tableMap, String workerStorageDirectory) {
        get("/count/:table", (request, response) -> {
            String tableName = request.params("table");
            String rowCount = null;
            try {
                if (!tableName.startsWith("pt-")) {
                    ConcurrentHashMap<String, RowVersioning> rows = tableMap.get(tableName);
                    if (rows != null) {
                        rowCount = String.valueOf(rows.size());
                        response.body(rowCount);
                    } else {
                        response.status(404, "Not Found");
                    }
                } else {
                    Path tableDirectoryPath = Paths.get(workerStorageDirectory + File.separator + tableName);
                    if (Files.exists(tableDirectoryPath) && Files.isDirectory(tableDirectoryPath) && Files.isReadable(tableDirectoryPath)){
                        try (Stream<Path> files = Files.walk(tableDirectoryPath)) {
                            rowCount = String.valueOf(files.filter(Files::isRegularFile).count());
                            response.body(rowCount);
                        } catch (Exception e) {
                            response.status(500, "Internal Server Error");
                            e.printStackTrace();
                        }
                    } else {
                        response.status(404, "Not Found");
                    }
                }
            } catch (Exception e) {
                response.status(404, "Not Found");
                e.printStackTrace();
                return "Table not found";
            }
            return null;
        });

        put("/rename/:table", (request, response) -> {
            String tableName = request.params("table");
            String newTableName = request.body();

            if (newTableName.isBlank()) {
                response.status(400, "BAD REQUEST");
                return "BAD REQUEST";
            }

            if (!tableName.startsWith("pt-")) {
                if (newTableName.startsWith("pt-")) {
                    response.status(400, "BAD REQUEST");
                    return "BAD REQUEST";
                }
                ConcurrentHashMap<String, RowVersioning> rows = tableMap.get(tableName);
                if (rows != null) {
                    ConcurrentHashMap<String, RowVersioning> previousValue = tableMap.putIfAbsent(newTableName, rows);
                    if (previousValue == null) {
                        tableMap.remove(tableName);
                        return "OK";
                    } else {
                        response.status(409, "CONFLICT");
                        return "CONFLICT";
                    }
                } else {
                    response.status(404, "NOT FOUND");
                    return "NOT FOUND";
                }
            } else {
                Path sourceDirectory = Paths.get(workerStorageDirectory, tableName);
                Path destinationDirectory = Paths.get(workerStorageDirectory, newTableName);
                if (!Files.exists(sourceDirectory)) {
                    response.status(404, "NOT FOUND");
                    return "NOT FOUND";
                } else if (Files.exists(destinationDirectory)) {
                    response.status(409, "CONFLICT");
                    return "CONFLICT";
                } else if (!newTableName.startsWith("pt-")) {
                    response.status(400, "BAD REQUEST");
                    return "BAD REQUEST";
                } else {
                    Files.move(sourceDirectory, destinationDirectory, StandardCopyOption.REPLACE_EXISTING);
                    return "OK";
                }
            }
        });

        put("/delete/:table", (request, response) -> {
            String tableName = request.params("table");

            if (tableName.startsWith("pt-")) {
                Path directory = Paths.get(workerStorageDirectory, tableName);
                if (Files.exists(directory)) {
                    try (Stream<Path> walk = Files.walk(directory)) {
                        walk.sorted(Comparator.reverseOrder())
                            .forEach(path -> {
                                try {
                                    Files.deleteIfExists(path);
                                } catch (IOException e) {
                                    throw new UncheckedIOException(e);
                                }
                            });
                        return "OK";
                    } catch (Exception e) {
                        response.status(500, "INTERNAL SERVER ERROR");
                        e.printStackTrace();
                        return "INTERNAL SERVER ERROR";
                    }
                } else {
                    response.status(404, "NOT FOUND");
                    return "NOT FOUND";
                }
            } else {
                ConcurrentHashMap<String, RowVersioning> removed = tableMap.remove(tableName);
                if (removed != null) {
                    return "OK";
                } else {
                    response.status(404, "NOT FOUND");
                    return "NOT FOUND";
                }
            }
        });

    }

    public static boolean isRowInRange(String key, String startRow, String endRowExclusive) {
        return (startRow == null || key.compareTo(startRow) >= 0) && 
           (endRowExclusive == null || key.compareTo(endRowExclusive) < 0);
    }
}
