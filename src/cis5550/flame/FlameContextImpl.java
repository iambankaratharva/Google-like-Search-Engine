package cis5550.flame;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import cis5550.kvs.KVSClient;
import cis5550.tools.HTTP;
import cis5550.tools.Hasher;
import cis5550.tools.Partitioner;
import cis5550.tools.Partitioner.Partition;
import cis5550.tools.Serializer;

public class FlameContextImpl implements FlameContext{
    private StringBuilder jobOutput;
    private int keyRangesPerWorker = -99999;
    private static KVSClient kvsClientStatic;
    private static AtomicInteger jobId = new AtomicInteger(1);

    public FlameContextImpl(String jarName, KVSClient kvsClient) {
        this.jobOutput = new StringBuilder();
        FlameContextImpl.kvsClientStatic = kvsClient;
    }

    @Override
    public KVSClient getKVS() {
        return kvsClientStatic;
    }

    @Override
    public void output(String s) {
        this.jobOutput.append(s);
    }

    public String getJobOutput() {
        return jobOutput.toString();
    }

    @Override
    public FlameRDD parallelize(List<String> list) throws Exception {
        String tableName = String.valueOf(jobId.getAndIncrement());
        for (int i = 0; i < list.size(); i++) {
            String rowKey = Hasher.hash(tableName + i);
            getKVS().put(tableName, rowKey, "value", list.get(i));
        }
        return new FlameRDDImpl(tableName, kvsClientStatic, this);
    }

    protected <T> String invokeOperation(String operation, byte[] lambda, Class<T> classType, String tableName, String argument) {
        String outputTable = "outputTable_" + UUID.randomUUID().toString().replace("-", "") + operation.split("/")[operation.split("/").length - 1];
        Partitioner partitioner = new Partitioner();
        if (keyRangesPerWorker != -99999) {
            partitioner.setKeyRangesPerWorker(keyRangesPerWorker);
        }
        try {
            addKVSWorkers(partitioner);
            addFlameWorkers(partitioner);
    
            Vector<Partition> partitions = partitioner.assignPartitions();
            Thread[] requestThreads = createAndStartRequestThreads(partitions, operation, tableName, outputTable, lambda, argument);
            waitForRequestThreadsCompletion(requestThreads);
    
            return outputTable;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
    
    private void addKVSWorkers(Partitioner partitioner) throws IOException {
        String firstWorkerId = kvsClientStatic.getWorkerID(0);
        String firstWorkerAddress = kvsClientStatic.getWorkerAddress(0);
        if (kvsClientStatic.numWorkers() == 1) {
            partitioner.addKVSWorker(firstWorkerAddress, firstWorkerId, null);
            partitioner.addKVSWorker(firstWorkerAddress, null, firstWorkerId);
        }
        if (kvsClientStatic.numWorkers() > 1) {
            for (int i = 0; i < kvsClientStatic.numWorkers() - 1; i++) {
                String workerId = kvsClientStatic.getWorkerID(i);
                String workerAddress = kvsClientStatic.getWorkerAddress(i);
                String nextWorkerId = kvsClientStatic.getWorkerID(i + 1);
                partitioner.addKVSWorker(workerAddress, workerId, nextWorkerId);
            }
            String lastWorkerId = kvsClientStatic.getWorkerID(kvsClientStatic.numWorkers() - 1);
            String lastWorkerAddress = kvsClientStatic.getWorkerAddress(kvsClientStatic.numWorkers() - 1);
            partitioner.addKVSWorker(lastWorkerAddress, lastWorkerId, null);
            partitioner.addKVSWorker(lastWorkerAddress, null, firstWorkerId);
        }
    }
    
    private void addFlameWorkers(Partitioner partitioner) {
        for (String worker : Coordinator.getWorkers()) {
            partitioner.addFlameWorker(worker);
        }
    }
    
    private Thread[] createAndStartRequestThreads(Vector<Partition> partitions, String operation, String tableName, String outputTable, byte[] lambda, String argument) {
        Thread[] requestThreads = new Thread[partitions.size()];
        for (int i = 0; i < partitions.size(); i++) {
            final int index = i;
            final Partition partition = partitions.get(index);
            requestThreads[i] = new Thread(() -> {
                try {
                    String url = partition.assignedFlameWorker + operation + "?inputTable=" + tableName + 
                    "&outputTable=" + outputTable + "&startKey=" + partition.fromKey + 
                    "&endKey=" + partition.toKeyExclusive + "&coordinator=" + kvsClientStatic.getCoordinator();
                    if (argument != null) {
                        url = url + "&argument=" + argument;
                    }
                    HTTP.Response res = HTTP.doRequest("POST", url, lambda);
                    if (res.statusCode() != 200) {
                        System.err.println("Request failed for partition " + index + " with status code: " + res.statusCode());
                        // throw new RuntimeException("The operation failed for partition " + index);
                    }
                } catch (Exception e) {
                    System.err.println("Exception in thread for partition " + index + ": " + e.getMessage());
                    e.printStackTrace();
                }
            }, "RequestThread-" + index);
            requestThreads[i].start();
        }
        return requestThreads;
    }
    
    
    private void waitForRequestThreadsCompletion(Thread[] requestThreads) throws InterruptedException {
        for (Thread requestThread : requestThreads) {
            requestThread.join();
        }
    }

    @Override
    public FlameRDD fromTable(String tableName, RowToString lambda) throws Exception {
        byte[] dataAsBytes = Serializer.objectToByteArray(lambda);
        String rddTableName = invokeOperation("/rdd/fromTable", dataAsBytes, FlameRDD.class, tableName, null);
        FlameRDDImpl rddTable = new FlameRDDImpl(rddTableName, kvsClientStatic, this);
        return rddTable;
    }

    @Override
    public void setConcurrencyLevel(int keyRangesPerWorker) {
        this.keyRangesPerWorker = keyRangesPerWorker;
    }
    
}
