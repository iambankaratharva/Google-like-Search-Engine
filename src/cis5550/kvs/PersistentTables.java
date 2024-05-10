package cis5550.kvs;

import static cis5550.tools.HTTP.doRequest;
import static cis5550.tools.KeyEncoder.decode;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;

import cis5550.kvs.KVSClient.WorkerEntry;
import cis5550.tools.HTTP;
import cis5550.tools.KeyEncoder;
import cis5550.webserver.Response;

public class PersistentTables {
    protected static void writeRowToPersistentTable(String tableName, String rowKey, String column, byte[] data, String workerStorageDirectory, KVSClient kvsClient, boolean isForwarded) throws IOException {
        File tableDirectory = new File(workerStorageDirectory + File.separator + tableName);
        if (!tableDirectory.exists()) {
            tableDirectory.mkdirs();
        }
        File rowFile = null;
        Row row = null;
        
        String encodedRowKey = KeyEncoder.encode(rowKey);
        if (encodedRowKey.length() >= 6) { // create sub-directory of rows with length >= 6
            File rowSubDirectory = new File(workerStorageDirectory + File.separator + tableName + File.separator + "__" + encodedRowKey.substring(0, 2));
            if (!rowSubDirectory.exists()) {
                rowSubDirectory.mkdirs();
            }
            rowFile = new File(rowSubDirectory, encodedRowKey);
        } else {
            rowFile = new File(tableDirectory, encodedRowKey);
        }
        
        if (rowFile.exists()) {
            try (FileInputStream fis = new FileInputStream(rowFile)) {
                row = Row.readFrom(fis);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            row = new Row(rowKey);
        }
        
        row.put(column, data);
    
        try (FileOutputStream fos = new FileOutputStream(rowFile)) {
            fos.write(row.toByteArray());
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (isForwarded == false) {
            WorkerEntry[] previousWorkers = getTwoPreviousWorkers(kvsClient.workers.elementAt(kvsClient.workerIndexForKey(rowKey)).id, kvsClient); // Get two previous workers
            if (previousWorkers != null) {
                forwardPut(kvsClient, tableName, encodedRowKey, data, column, previousWorkers);
            }
        }
    }

    protected static void writeWholeRowToPersistentTable(String tableName, Row row, String workerStorageDirectory, KVSClient kvsClient, boolean isForwarded) throws IOException
    {
        File tableDirectory = new File(workerStorageDirectory + File.separator + tableName);
        if (!tableDirectory.exists()) {
            tableDirectory.mkdirs();
        }
        File rowFile = null;

        String encodedRowKey = KeyEncoder.encode(row.key());
        if (encodedRowKey.length() >= 6) { // create sub-directory of rows with length >= 6
            File rowSubDirectory = new File(workerStorageDirectory + File.separator + tableName + File.separator + "__" + encodedRowKey.substring(0, 2));
            if (!rowSubDirectory.exists()) {
                rowSubDirectory.mkdirs();
            }
            rowFile = new File(rowSubDirectory, encodedRowKey);
        } else {
            rowFile = new File(tableDirectory, encodedRowKey);
        }

        try (FileOutputStream fos = new FileOutputStream(rowFile)) {
            fos.write(row.toByteArray());
        } catch (Exception e) {
            e.printStackTrace();
        }

        if (isForwarded == false) {
            WorkerEntry[] previousWorkers = getTwoPreviousWorkers(kvsClient.workers.elementAt(kvsClient.workerIndexForKey(row.key())).id, kvsClient); // Get two previous workers
            if (previousWorkers != null) {
                forwardPutWholeRow(kvsClient, tableName, row, previousWorkers);
            }
        }
    }

    public static void writeMultipleRowsToPersistentTable(String tableName, List<Row> rows, String workerStorageDirectory, KVSClient kvsClient) {
        for (Row row : rows) {
            try {
                writeWholeRowToPersistentTable(tableName, row, workerStorageDirectory, kvsClient, false);
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("Error in putBatch for " + tableName);
            }
        }
    }

    private static void forwardPut(KVSClient kvsClient, String tableName, String rowKey, byte[] data, String column, WorkerEntry[] previousWorkers) throws IOException {
        for (WorkerEntry previousWorker : previousWorkers) {
            String target = "http://"+previousWorker.address+"/data/"+tableName+"/"+java.net.URLEncoder.encode(rowKey, "UTF-8")+"/"+java.net.URLEncoder.encode(column, "UTF-8")+"?forwarded=true";
            // System.out.println("Forward put to: " + target);
            byte[] response = HTTP.doRequest("PUT", target, data).body();
            String result = new String(response);
            if (!result.equals("OK")) {
                throw new RuntimeException("PUT returned something other than OK: "+ result);
            }
        }
    }

    private static void forwardPutWholeRow(KVSClient kvsClient, String tableName, Row row, WorkerEntry[] previousWorkers) throws IOException {
        for (WorkerEntry previousWorker : previousWorkers) {
            String target = "http://"+previousWorker.address+"/data/"+tableName+"?forwarded=true";
            // System.out.println("Forward put to: " + target);
            byte[] response = HTTP.doRequest("PUT", target, null).body();
            String result = new String(response);
            if (!result.equals("OK")) {
                throw new RuntimeException("PUT returned something other than OK: "+ result);
            }
        }
    }

    public static WorkerEntry[] getTwoPreviousWorkers(String workerId, KVSClient kvsClient) throws IOException {
        int index = -1;
        for (int i = 0; i < kvsClient.workers.size(); i++) {
            if (kvsClient.workers.get(i).id.equals(workerId)) {
                index = i;
                break;
            }
        }

        WorkerEntry[] result = new WorkerEntry[2];
        if (index != -1) { // Ensure the worker is found
            int firstPrevIndex = (index + kvsClient.workers.size() - 1) % kvsClient.workers.size();
            int secondPrevIndex = (index + kvsClient.workers.size() - 2) % kvsClient.workers.size();

            // Get the previous workers
            result[0] = kvsClient.workers.get(secondPrevIndex);
            result[1] = kvsClient.workers.get(firstPrevIndex);
        } else {
            return null; // Worker not found
        }
        return result;
    }

    protected static byte[] readFromPersistentTable(String tableName, String rowKey, String column, Response response, String workerStorageDirectory) {
        String encodedRowKey = KeyEncoder.encode(rowKey);
        File rowFile = null;
        if (encodedRowKey.length() >= 6) {
            rowFile = new File(workerStorageDirectory + File.separator + tableName + File.separator + "__" + encodedRowKey.substring(0, 2) + File.separator + encodedRowKey);
        } else {
            rowFile = new File(workerStorageDirectory + File.separator + tableName + File.separator + encodedRowKey);
        }
        Row row = null;
        byte[] data = null;

        if (rowFile.exists()) {
            try (FileInputStream fis = new FileInputStream(rowFile)) {
                row = Row.readFrom(fis);
                data = row.getBytes(column);
                if (data == null) {
                    response.status(404, "Not Found");
                    return null;
                }
                response.bodyAsBytes(data);
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            response.status(404, "Not Found");
        }
        return data;
    }

    private static File determineRowFile(String encodedRowKey, String tableName, String workerStorageDirectory) {
        if (encodedRowKey.length() >= 6) {
            return new File(workerStorageDirectory + File.separator + tableName + File.separator + "__" + encodedRowKey.substring(0, 2) + File.separator + encodedRowKey);
        } else {
            return new File(workerStorageDirectory + File.separator + tableName + File.separator + encodedRowKey);
        }
    }

    protected static void readWholeRowFromPersistentTable(String tableName, String rowKey, Response response, String workerStorageDirectory) {
        File rowFile = null;
        String encodedRowKey = KeyEncoder.encode(rowKey);
        if (encodedRowKey.length() >= 6) {
            rowFile = new File(workerStorageDirectory + File.separator + tableName + File.separator + "__" + encodedRowKey.substring(0, 2) + File.separator + encodedRowKey);
        } else {
            rowFile = new File(workerStorageDirectory + File.separator + tableName + File.separator + encodedRowKey);
        }
        Row row = null;
    
        if (rowFile.exists() && rowFile != null) {
            try (FileInputStream fis = new FileInputStream(rowFile)) {
                row = Row.readFrom(fis);
                response.bodyAsBytes(row.toByteArray());
            } catch (Exception e) {
                response.status(404, "Not Found");
                e.printStackTrace();
            }
        } else {
            response.status(404, "Not Found");
        }
    }

    protected static void streamDataFromPersistentTable(String tableName, Response response, String startRow, String endRowExclusive, String workerStorageDirectory) throws Exception {
        Path tableDirectory = Paths.get(workerStorageDirectory, tableName);
        response.header("content-type", "text/plain");
    
        if (Files.exists(tableDirectory) && Files.isDirectory(tableDirectory) && Files.isReadable(tableDirectory)) {
            try (Stream<Path> stream = Files.walk(tableDirectory)) {
                stream.filter(Files::isRegularFile)
                    .filter(path -> Files.isReadable(path))
                    .sorted(Comparator.comparing(Path::getFileName))
                    .forEach(path -> {
                        File file = path.toFile();
                        if (cis5550.generic.Worker.isRowInRange(file.getName(), startRow, endRowExclusive)) {
                            try (FileInputStream fis = new FileInputStream(file)) {
                                Row row = Row.readFrom(fis);
                                response.write(row.toByteArray());
                                response.write("\n".getBytes());
                            } catch (Exception e) {
                                System.err.println("Error processing file: " + file.getAbsolutePath());
                                e.printStackTrace();
                            }
                        }
                    });
            }
            response.write("\n".getBytes());
        } else {
            response.status(404, "Not Found");
        }
    }

    protected static void streamDataHashFromNextTwoWorkers(String currentWorkerAddr, String currentWorkerId, KVSClient kvsClient, String workerStorageDirectory) throws IOException, NoSuchAlgorithmException {
        WorkerEntry[] twoNextWorkers = getTwoNextWorkers(currentWorkerId, kvsClient);
        File[] filesOfTwoNextWorkers = getFilesOfTwoNextWorkers(twoNextWorkers);

        if (filesOfTwoNextWorkers.length != 0) {
            for (File file : filesOfTwoNextWorkers) {
                if (!file.getName().isBlank() && checkIfWorkerIsPrimaryOwnerOfFile(file, twoNextWorkers, kvsClient)) {
                    try (FileInputStream fis = new FileInputStream(file)) {
                        // System.out.println("File to be hashed: " + file.getName() + ". Current Worker: " + currentWorkerAddr);
                        StringBuilder fileContentBuilder = new StringBuilder();
                        MessageDigest digest = MessageDigest.getInstance("SHA-256");
                        byte[] fileBytes = new byte[1024];
                        int bytesRead;
                        
                        while ((bytesRead = fis.read(fileBytes)) != -1) {
                            digest.update(fileBytes, 0, bytesRead);
                            fileContentBuilder.append(new String(fileBytes, 0, bytesRead));
                        }
                        
                        byte[] hashBytes = digest.digest(); // hash computation
                        StringBuilder hashedRowStringBuilder = new StringBuilder();
                        for (byte hashByte : hashBytes) { // conversion of digest (hash bytes) to hexadecimal format
                            hashedRowStringBuilder.append(String.format("%02x", hashByte));
                        }
                        String hashedContent = hashedRowStringBuilder.toString();
                        String unhashedContent = fileContentBuilder.toString();

                        checkForHashInCurrentWorker(file, hashedContent, unhashedContent, workerStorageDirectory);
                        
                    } catch (Exception e) {
                        // System.err.println("Error processing file in nextTwoWorker: " + file.getAbsolutePath());
                        e.printStackTrace();
                    }
                }
            }
        } else {
            // System.out.println("No files found!");
        }
    }

    private static void checkForHashInCurrentWorker(File file, String hashedContent, String unhashedContent, String workerStorageDirectory) throws IOException {
        String tableAndRowName = extractPtPath(file.getAbsolutePath());
        String absoluteTableAndRowPath = workerStorageDirectory + "/" + tableAndRowName;
        File absoluteTableAndRowPathFile = new File(absoluteTableAndRowPath);
        if (absoluteTableAndRowPathFile.exists() && absoluteTableAndRowPathFile.isFile() && absoluteTableAndRowPathFile.canRead()) {
            try (FileInputStream fis = new FileInputStream(absoluteTableAndRowPathFile)) {
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                byte[] fileBytes = new byte[1024];
                int bytesRead;
                
                while ((bytesRead = fis.read(fileBytes)) != -1) {
                    digest.update(fileBytes, 0, bytesRead);
                }
                
                byte[] hashBytes = digest.digest(); // hash computation of current worker's file
                StringBuilder hashedRowStringBuilder = new StringBuilder();
                for (byte hashByte : hashBytes) { // conversion of digest (hash bytes) to hexadecimal format
                    hashedRowStringBuilder.append(String.format("%02x", hashByte));
                }
                String currentHashedContent = hashedRowStringBuilder.toString();
    
                if (!currentHashedContent.equals(hashedContent)) {
                    // System.out.println("File exists, content hash not matched");
                    // System.out.println("Original location: " + file.getAbsolutePath());
                    // since content is not equal, write unhashedContent
                    try (FileOutputStream fos = new FileOutputStream(absoluteTableAndRowPathFile)) {
                        byte[] buffer = unhashedContent.getBytes();
                        fos.write(buffer, 0, buffer.length);
                        // System.out.println("Edited with unhashed content at\n");
                        // System.out.println(absoluteTableAndRowPath);
                    } catch (Exception e) {
                        // System.err.println("Error writing unhashed content to file: " + absoluteTableAndRowPath);
                        e.printStackTrace();
                    }
                } else {
                    // System.out.println("Content matched");
                }
            } catch (Exception e) {
                // System.err.println("Error processing file: " + absoluteTableAndRowPath);
                e.printStackTrace();
            }
        } else {
            // Ensure that the parent directories exist
            File parentDir = absoluteTableAndRowPathFile.getParentFile(); // Get the parent directory
            if (parentDir != null && !parentDir.exists()) {
                if (parentDir.mkdirs()) {
                    // System.out.println("Successfully created parent directories.");
                } else {
                    // System.err.println("Failed to create parent directories.");
                }
            }
            absoluteTableAndRowPathFile.createNewFile(); // No file found in current worker
            try (FileOutputStream fos = new FileOutputStream(absoluteTableAndRowPathFile)) {
                byte[] buffer = unhashedContent.getBytes();
                fos.write(buffer, 0, buffer.length);
                // System.out.println("Successfully created file with unhashed content at \n" + absoluteTableAndRowPathFile);
            } catch (Exception e) {
                // System.err.println("Error writing unhashed content to new file: " + absoluteTableAndRowPath);
                e.printStackTrace();
            }
        }
    }

    public static String extractPtPath(String fullPath) {
        if (fullPath == null || fullPath.isEmpty()) {
            return "";
        }
        String[] parts = fullPath.split("/");
        // Iterate through the parts to find the segment starting with "pt-"
        for (String part : parts) {
            if (part.startsWith("pt-")) {
                // Join the remaining parts starting from the "pt-" part
                StringBuilder sb = new StringBuilder(part);
                int index = fullPath.indexOf(part) + part.length();
                if (index < fullPath.length()) {
                    sb.append(fullPath.substring(index));
                }
                return sb.toString();
            }
        }
        return "";
    }

    private static File[] getFilesOfTwoNextWorkers(WorkerEntry[] twoNextWorkers) throws IOException {
        List<File> fileList = new ArrayList<>();
        for (WorkerEntry worker : twoNextWorkers) {
            String route = "http://"+worker.address+"/FileNames";
            HTTP.Response response;
            try {
                response = doRequest("GET", route, null);
                if (response.statusCode() == 200) {
                    if (response.body() != null) {
                        String[] filePaths = new String(response.body()).split(",");
                        for (String path : filePaths) {
                            path = path.replaceAll("[^a-zA-Z0-9\\._/\\-]", "");
                            fileList.add(new File(path.trim()));
                        }
                    } else {
                        // System.out.println("Null from GET route");
                    }
                }
            } catch (Exception e) {
                // System.out.println("Some worker might be down!");
                e.getMessage();
            }
        }
        return fileList.toArray(new File[0]);
    }

    private static boolean checkIfWorkerIsPrimaryOwnerOfFile(File file, WorkerEntry[] twoNextWorkers, KVSClient kvsClient) throws IOException {
        WorkerEntry chosenWorker = kvsClient.workers.elementAt(kvsClient.workerIndexForKey(decode(file.getName())));
        if (twoNextWorkers != null && (chosenWorker.id.equals(twoNextWorkers[0].id) || chosenWorker.id.equals(twoNextWorkers[1].id))) {
            return true;
        }
        return false;
    }

    private static WorkerEntry[] getTwoNextWorkers(String workerId, KVSClient kvsClient) throws IOException {
        int index = -1;
        for (int i = 0; i < kvsClient.workers.size(); i++) {
            if (kvsClient.workers.get(i).id.equals(workerId)) {
                index = i;
                break;
            }
        }
    
        WorkerEntry[] result = new WorkerEntry[2];
        if (index != -1) { // Ensure the worker is found
            int firstNextIndex = (index + 1) % kvsClient.workers.size(); // The next index in the list
            int secondNextIndex = (index + 2) % kvsClient.workers.size(); // The second next index in the list
    
            // Get the next workers
            result[0] = kvsClient.workers.get(firstNextIndex);
            result[1] = kvsClient.workers.get(secondNextIndex);
        } else {
            return null; // Worker not found
        }
        return result;
    }
}
