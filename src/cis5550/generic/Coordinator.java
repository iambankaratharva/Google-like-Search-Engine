package cis5550.generic;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import static cis5550.webserver.Server.*;

public class Coordinator {
    private static final ConcurrentHashMap<String, String> workerAddressMap = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<String, Long> lastPingMap = new ConcurrentHashMap<>();
    private static final long INACTIVE_THRESHOLD = 15 * 1000; // 15 seconds in milliseconds

    protected static void registerRoutes() {
        System.out.println("Registering routes for /ping and /workers");
        get("/ping", (req, res) -> {
            if (!req.queryParams().contains("id") || !req.queryParams().contains("port")) {
                res.status(400, "Bad Request");
                return "400 Bad Request";
            }

            String id = req.queryParams("id");
            String ip = req.ip();
            String portNumber = req.queryParams("port");
            workerAddressMap.put(id, ip+":"+portNumber);
            lastPingMap.put(id, System.currentTimeMillis()); // Update last ping time
            return "OK";
        });

        get("/workers", (req, res) -> {
            // Check for inactivity before responding
            removeInactiveWorkers();
            res.header("content-type", "text/plain");
            
            StringBuilder response = new StringBuilder();
            response.append(workerAddressMap.size()).append("\n");
            for (Map.Entry<String, String> entry : workerAddressMap.entrySet()) {
                response.append(entry.getKey()).append(",").append(entry.getValue()).append("\n");
            }
            return response.toString();
        });
    }

    private static void removeInactiveWorkers() {
        long currentTime = System.currentTimeMillis();
        for (Map.Entry<String, Long> entry : lastPingMap.entrySet()) {
            if (currentTime - entry.getValue() > INACTIVE_THRESHOLD) {
                workerAddressMap.remove(entry.getKey());
                lastPingMap.remove(entry.getKey());
                System.out.println("Worker " + entry.getKey() + " removed due to inactivity.");
            }
        }
    }

    protected static String workerTable() {
        StringBuilder table = new StringBuilder();
        table.append("<html><body><table><tr><th>Worker ID</th><th>IP Address</th><th>Port Number</th><th>Link</th></tr>");
        
        for (Map.Entry<String, String> entry : workerAddressMap.entrySet()) {
            String id = entry.getKey();
            String[] parts = entry.getValue().split(":");
            String ip = parts[0];
            String port = parts[1];
            
            String workerLink = "<a href=\"http://" + entry.getValue() + "\">Link</a>";
            
            table.append("<tr><td>").append(id).append("</td><td>").append(ip).append("</td><td>").append(port).append("</td><td>").append(workerLink).append("</td></tr>");
        }
        
        table.append("</table></body></html>");
        return table.toString();
    }

    public static List<String> getWorkers() {
        List<String> workerList = new ArrayList<>();
        workerAddressMap.forEach((id, workerInfo) -> {
            workerList.add(workerInfo);
        });
        return workerList;
    }
    
}
