package cis5550.webserver;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import javax.net.ServerSocketFactory;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

public class Server {
    private static Server instance = null;
    private static boolean flag = false;
    public boolean securePortFlag = false;
    public int serverSocketPort = 80;
    public int serverSocketSecurePort = 443;
    public String directoryName = "";
    public Map<String, Map<String, Route>> routes = new ConcurrentHashMap<>();
    public static List<filterLambda> beforeLambdas = new ArrayList<>();
    public static List<filterLambda> afterLambdas = new ArrayList<>();
    public static ConcurrentHashMap<String, SessionImpl > sessionHashMap = new ConcurrentHashMap<>();

    public static void startServerIfNeeded() {
        if (instance == null) {
            instance = new Server();
        }
        if (flag == false) {
            flag = true;
            Thread processRequestInstance = new Thread(instance::run);
            processRequestInstance.start();
        }
    }

    public static void port(int serverSocketPort) {
        if (instance == null) {
            instance = new Server();
        }
        instance.serverSocketPort = serverSocketPort;
    }

    public static void securePort(int serverSocketSecurePort) {
        if (instance == null) {
            instance = new Server();
        }
        instance.serverSocketSecurePort = serverSocketSecurePort;
        instance.securePortFlag = true;
    }

    public static class staticFiles {
        public static void location(String s) {
            startServerIfNeeded();
            instance.directoryName = s;
        }
    }

    public static void addToRoutingTable(String method, String path, Route route) {
        instance.routes.computeIfAbsent(method, k -> new ConcurrentHashMap<>()).put(path, route);
    }

    public static void get (String path, Route route) {
        startServerIfNeeded();
        addToRoutingTable("GET", path, route);
    }

    public static void post (String path, Route route) {
        startServerIfNeeded();
        addToRoutingTable("POST", path, route);
    }

    public static void put (String path, Route route) {
        startServerIfNeeded();
        addToRoutingTable("PUT", path, route);
    }

    public static void serverLoop(ServerSocket server, BlockingQueue<Map.Entry<Socket, Boolean>> socketQueue, Boolean isSecure) {
        try {
            while (true) {
                Socket clientSocket = server.accept();
                socketQueue.put(new ConcurrentHashMap.SimpleEntry<>(clientSocket, isSecure));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // Before and After filtering
    public interface filterLambda {
        void execute(Request request, Response response);
    }

    public static void before (filterLambda lambda) {
        instance.beforeLambdas.add(lambda);
    }

    public static void after (filterLambda lambda) {
        instance.afterLambdas.add(lambda);
    }

    // Generates a random session ID if the request doesn't have one
    private static String generateRandomSessionId() {
        final SecureRandom random = new SecureRandom();
        final String SESSION_ID_CHARACTERS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 20; i++) {
            int index = random.nextInt(SESSION_ID_CHARACTERS.length());
            sb.append(SESSION_ID_CHARACTERS.charAt(index));
        }
        return sb.toString();
    }

    public SessionImpl sessionNew() {
        SessionImpl session = new SessionImpl(generateRandomSessionId());
        sessionHashMap.put(session.id(), session);
        return session;
    }

    private static final int NUM_WORKERS = 100;
    private static final BlockingQueue<Map.Entry<Socket, Boolean>> socketQueue = new LinkedBlockingQueue<>(10);

    public void run() {
        // Runnable
        Runnable workerThreadRunnable = new ProcessRequest(socketQueue, instance, sessionHashMap);

        // Start worker threads
        for (int i = 0; i < NUM_WORKERS; i++) {
            Thread thread = new Thread(workerThreadRunnable);
            thread.start();
        }

        ServerSocket serverSocketHttp;
        try {
            serverSocketHttp = new ServerSocket(serverSocketPort);
            Thread httpThread = new Thread(() -> serverLoop(serverSocketHttp, socketQueue, false));
            httpThread.start();
            System.out.println("HTTP Server started on port: " + serverSocketPort);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            String pwd = "secret";
            KeyStore keyStore = KeyStore.getInstance("JKS");
            keyStore.load(new FileInputStream("keystore.jks"), pwd.toCharArray());
            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
            keyManagerFactory.init(keyStore, pwd.toCharArray());
            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagerFactory.getKeyManagers(), null, null);
            ServerSocketFactory factory = sslContext.getServerSocketFactory();
            ServerSocket serverSocketTLS = factory.createServerSocket(serverSocketSecurePort);

            Thread httpsThread = new Thread(() -> serverLoop(serverSocketTLS, socketQueue, true));
            httpsThread.start();
            System.out.println("HTTPS Server started on port: " + serverSocketSecurePort);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (KeyStoreException | NoSuchAlgorithmException | CertificateException | UnrecoverableKeyException | KeyManagementException | IOException e) {
            e.printStackTrace();
        }
    }
}