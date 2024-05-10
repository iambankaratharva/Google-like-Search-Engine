package cis5550.webserver;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URLDecoder;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Locale;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;

public class ProcessRequest implements Runnable {
    private BlockingQueue<Map.Entry<Socket, Boolean>> socketQueue;
    private Server serverInstance;
    private ConcurrentHashMap<String, SessionImpl> sessionHashMap = new ConcurrentHashMap<>();
    public ProcessRequest(BlockingQueue<Entry<Socket, Boolean>> socketQueue, Server serverInstance, ConcurrentHashMap<String, SessionImpl> sessions) {
        this.socketQueue = socketQueue;
        this.serverInstance = serverInstance;
        this.sessionHashMap = sessions;
    }

    @Override
    public void run() {
        while (true) {
            try {
                Map.Entry<Socket, Boolean> entry = socketQueue.take();
                Socket clientSocket = entry.getKey();
                boolean isSecure = entry.getValue();
                handleRequests(clientSocket, serverInstance, sessionHashMap, isSecure);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void handleRequests(Socket clientSocket, Server serverInstance, ConcurrentHashMap<String, SessionImpl> sessionHashMap, Boolean isSecure){
        try (InputStream in = clientSocket.getInputStream();
        OutputStream out = clientSocket.getOutputStream()) {
            while (true) {
                ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                int currentByte;
                boolean EOF = false;
                byte[] doubleCRLF = {13, 10, 13, 10};
                while (true) {
                    currentByte = in.read();
                    if (currentByte == -1) { // Exit loop when End of File is encountered
                        EOF = true;
                        break;
                    }
                    byteArrayOutputStream.write(currentByte);
                    byte[] byteArray = byteArrayOutputStream.toByteArray();
                    int len = byteArray.length;

                    if (len >= 4 && Arrays.equals(Arrays.copyOfRange(byteArray, len-4, len), doubleCRLF)) {
                        // Exit loop when double CRLF is encountered (after Header)
                        break;
                    }
                }
                if (EOF) {
                    break;
                }

                // Convert {Request line + Header} from Bytes to String
                ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
                InputStreamReader inputStreamReader = new InputStreamReader(byteArrayInputStream, "ASCII");
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                String requestLine = bufferedReader.readLine();

                String httpMethod = null;
                String httpVersion = null;
                String fileName = null;
                try {
                    String[] requestParts = requestLine.split(" ");
                    int contentLength = -1;
                    String contentType = null;
                    String requestContentType = null;
                    String host = null;
                    Date ifModifiedSinceDate = null;
                    SessionImpl session = null;
                    
                    // Parse Header and calculate Content-Length
                    StringBuilder requestHeaderBuilder = new StringBuilder();
                    Map<String, String> requestHeaderMap = new ConcurrentHashMap<>();
                    String headerLine = bufferedReader.readLine();
                    while (headerLine != null && !headerLine.isEmpty()) {
                        requestHeaderMap.put(headerLine.split(":")[0].trim().toLowerCase(), headerLine.split(":")[1].trim());
                        requestHeaderBuilder.append(headerLine).append("\r\n");
                        headerLine = bufferedReader.readLine();
                    }

                    String requestHeader = requestHeaderBuilder.toString().trim();
                    if (headerLine.isEmpty()) { // double CRLF is encountered
                        contentLength = parseContentLength(requestHeader);
                        ifModifiedSinceDate = parseIfModifiedSinceDate(requestHeader);
                        host = parseHost(requestHeader);
                        requestContentType = parseContentTypeInRequest(requestHeader);
                    }

                    byte[] requestBodyRaw = new byte[contentLength];
                    currentByte = in.read(requestBodyRaw);

                    if (requestParts.length < 3) {
                        sendErrorResponse(400, "Bad Request", 
                        "Your request is malformed or incorrect.", clientSocket, out);
                    } else {
                        httpMethod = requestParts[0];
                        fileName = requestParts[1];
                        httpVersion = requestParts[2];
                    }

                    if (httpMethod == null || // method
                    fileName == null || // url
                    !httpVersion.split("/")[0].trim().equals("HTTP") || // protocol
                    host == null) { // host)
                        sendErrorResponse(400, "Bad Request", 
                        "Your request is malformed or incorrect.", clientSocket, out);
                    } else if (!httpMethod.matches("GET|HEAD|PUT|POST")) {
                        sendErrorResponse(501, "Not Implemented", 
                        "The server does not support the requested method; " +
                        "the client should use GET, HEAD, POST, or PUT.", clientSocket, out);
                    } else if ((serverInstance.directoryName+fileName).contains("..")) {
                        sendErrorResponse(403, "Forbidden", 
                        "Action not allowed.", clientSocket, out);
                    } else if (!httpVersion.equals("HTTP/1.1")){
                        sendErrorResponse(505, "HTTP Version Not Supported", 
                        "The server does not support the HTTP protocol version used by the client.",
                        clientSocket, out);
                    } else {
                        // Dynamic routing
                        // Check if method matches from current request & path pattern matches the URL
                        boolean dynamicRouting = false;
                        Map<String, Route> methodRoute = serverInstance.routes.get(httpMethod);
                        Map<String, String> pathParams = new ConcurrentHashMap<>();
                        Route matchedRoute = null;
                        if (serverInstance.routes.containsKey(httpMethod)) {
                            for (String pathPattern : methodRoute.keySet()) {
                                if (doesUrlMatchPathPattern(fileName.split("\\?")[0], pathPattern, pathParams)) {
                                    dynamicRouting = true;
                                    matchedRoute = methodRoute.get(pathPattern);
                                    break;
                                }
                            }
                        }
                        // Parse query parameters
                        Map<String, String> queryParams = new ConcurrentHashMap<>();
                        try {
                            if (requestContentType != null && requestContentType.equalsIgnoreCase("application/x-www-form-urlencoded")) {
                                String requestBodyRawString = new String(requestBodyRaw);
                                String[] requestBodyRawStringParts = requestBodyRawString.split("&");

                                for (String eachQueryParam : requestBodyRawStringParts) {
                                    String[] keyValue = eachQueryParam.split("=");
                                    if (keyValue.length == 2) {
                                        addParam(queryParams, URLDecoder.decode(keyValue[0], "UTF-8"), URLDecoder.decode(keyValue[1], "UTF-8"));
                                    }
                                }

                            }
                            if (fileName != null && fileName.contains("?")) {
                                String queryString = fileName.substring(fileName.indexOf("?") + 1);
                                String[] queryParamsString = queryString.split("&");

                                for (String eachQueryParam : queryParamsString) {
                                    String[] keyValue = eachQueryParam.split("=");
                                    if (keyValue.length == 2) {
                                        addParam(queryParams, URLDecoder.decode(keyValue[0], "UTF-8"), URLDecoder.decode(keyValue[1], "UTF-8"));
                                    }
                                }
                            }
                        } catch (Exception e) {
                            String responseBody = "Error parsing query parameters";
                            String response = "HTTP/1.1 400 Bad Request" + "\r\n Content-Length: " + responseBody.length() + "\r\n\r\n" + responseBody;
                            out.write(response.getBytes());
                        }

                        if (dynamicRouting) {
                            Session currentSession = null;
                            String currentSessionId = null;
                            String sessionIdInRequest = parseSessionId(requestHeader);
                            if (sessionIdInRequest != null) { // Session ID is available in request
                                currentSessionId = sessionIdInRequest;
                                currentSession = sessionHashMap.get(currentSessionId);
                            }
                            RequestImpl request = new RequestImpl(httpMethod, fileName, httpVersion, requestHeaderMap, 
                            queryParams, pathParams, (InetSocketAddress)clientSocket.getRemoteSocketAddress(), 
                            requestBodyRaw, currentSession, serverInstance);
                            ResponseImpl response = new ResponseImpl(httpVersion, out);
                            // Execute before lambdas
                            for (Server.filterLambda lambda : serverInstance.beforeLambdas) {
                                lambda.execute(request, response);
                            }
                            try {
                                if (!response.fetchHaltStatus()) {
                                    Object result = matchedRoute.handle(request, response);
                                    if (request.isSessionCalled()) {
                                        currentSession = request.session();
                                        currentSessionId = currentSession.id();
                                    }
                                    session = getSession(requestHeader, currentSession, currentSessionId, request, serverInstance);
                                    int responseContentLength = 0;
                                    byte[] responseContent = null;
                                    if (response.fetchWriteStatus()) {
                                        break;
                                    }
                                    if (result != null) { // write not called, result not null -> send result of handler
                                        responseContentLength = result.toString().length();
                                        response.header("Content-Length", Integer.toString(responseContentLength));
                                        responseContent = result.toString().getBytes();
                                    } else { // write not called, result null -> get body() / bodyAsBytes()
                                        if (response.fetchBody() == null) {
                                            responseContent = null;
                                            response.header("Content-Length", "0");
                                        } else { // body / bodyasbytes
                                            String responseContentString = new String(response.fetchBody());
                                            int responseContentStringLength = responseContentString.length();
                                            responseContent = response.fetchBody();
                                            response.header("Content-Length", Integer.toString(responseContentStringLength));
                                        }
                                    }
                                    boolean contentTypeFlag = false;
                                    for (Map.Entry<String, List<String>> entry : response.fetchHeaders().entrySet()) {
                                        if (entry.getKey().equalsIgnoreCase("Content-Type")) {
                                            contentTypeFlag = true;
                                            break;
                                        }
                                    }
                                    if (contentTypeFlag == false) {
                                        response.type("text/html");
                                    }
                                    
                                    String responseStatusLine = httpVersion + " " + response.fetchStatusCode() + " " + response.fetchReasonPhrase() + "\r\n";
                                    out.write(responseStatusLine.getBytes());
                                    response.header("Server", "cis5550.webserver");
                                    if(session != null && session.isNewSession()) {
                                        String cookieValue = "SessionID=" + session.id() + "; HttpOnly; SameSite=Strict";
                                        if (isSecure) {
                                            cookieValue += "; Secure";
                                        }
                                        response.header("Set-Cookie", cookieValue);
                                    }
                                    for (String headerField : response.fetchHeaders().keySet()) {
                                        for (String headerValues : response.fetchHeaders().get(headerField)) {
                                            String responseHeaderLine = headerField + ": " + headerValues + "\r\n";
                                            out.write(responseHeaderLine.getBytes());
                                        }
                                    }
                                    out.write("\r\n".getBytes());
                                    out.write(responseContent);
                                    out.flush();
                                }
                                // Execute after lambdas
                                for (Server.filterLambda lambda : serverInstance.afterLambdas) {
                                    lambda.execute(request, response);
                                }
                            } catch (Exception e) {
                                String errorResponse = "HTTP/1.1 500 Internal Server Error";
                                String errorMessage = e.getMessage();
                                errorResponse += "\r\n Content-Length: " + errorMessage.length() + "\r\n\r\n" + errorMessage;
                                out.write(errorResponse.getBytes());
                                e.printStackTrace();
                                break;
                            }
                        } else { // Static routing, if dynamic is skipped
                            if (httpMethod.equals("POST") || httpMethod.equals("PUT")) {
                                sendErrorResponse(405, "Not Allowed", 
                                "Not permitted to create or update the resource.", clientSocket, out);
                            } else {
                                File requestedFile = new File(serverInstance.directoryName + fileName);
                                contentType = parseContentType(requestedFile.toPath().toString());

                                if (!requestedFile.exists() || requestedFile.isDirectory()) {
                                    sendErrorResponse(404, "Not Found", 
                                "File not found.", clientSocket, out);
                                    break;
                                } else {
                                    long lastModified = requestedFile.lastModified();
                                    if (ifModifiedSinceDate != null && 
                                    lastModified <= ifModifiedSinceDate.getTime() &&
                                    ifModifiedSinceDate.getTime() <= System.currentTimeMillis()){
                                        // File not modified
                                        sendErrorResponse(304, "Not Modified", 
                                        "", clientSocket, out); // status code 304 does not have response body
                                    } else { // Sending file content, file has been modified, check for Range request
                                        long requestedFileLength = requestedFile.length();

                                        String[] requestedRange = parseRangeRequest(requestHeader, requestedFile,  requestedFileLength);
                                        if (requestedRange[0].equals("valid")) { // Range Request valid, 206 status code
                                            String lowerByte = requestedRange[1].trim();
                                            String upperByte = requestedRange[2].trim();
                                            long bytesRemaining = Long.parseLong(upperByte) - Long.parseLong(lowerByte) + 1;
                                            String response = "HTTP/1.1 206 Partial Content\r\n" +
                                            "Content-Range: " + "bytes " + lowerByte + "-" + upperByte + "/" + (requestedFileLength) + "\r\n" +
                                            "Content-Length: " + bytesRemaining + "\r\n" +
                                            "Server: cis5550.webserver" + "\r\n" +
                                            "Content-Type: " + contentType + "\r\n\r\n";
                                            out.write(response.getBytes());
                                            if (httpMethod.equals("GET")) {
                                                int bytesOfFileRead;
                                                byte[] buffer = new byte[1024];
                                                try (FileInputStream fileInputStream = new FileInputStream(requestedFile)) {
                                                    fileInputStream.skip(Long.parseLong(lowerByte)); // skip to start point of lower byte
                                                    while ((bytesOfFileRead = fileInputStream.read(buffer, 0, (int) Math.min(buffer.length, bytesRemaining))) != -1 
                                                    && bytesRemaining > 0) {
                                                        out.write(buffer, 0, bytesOfFileRead);
                                                        out.flush();
                                                        bytesRemaining -= bytesOfFileRead;
                                                    }
                                                }
                                            }
                                        } else { // No (or invalid) range request, 200 status code
                                            String response = "HTTP/1.1 200 OK\r\n" +
                                            "Content-Length: " + (requestedFileLength) + "\r\n" +
                                            "Server: cis5550.webserver" + "\r\n" +
                                            "Content-Type: " + contentType + "\r\n\r\n";
                                            out.write(response.getBytes());
                                            if (httpMethod.equals("GET")) {
                                                int bytesOfFileRead;
                                                byte[] buffer = new byte[1024];
                                                try (FileInputStream fileInputStream = new FileInputStream(requestedFile)) {
                                                    while ((bytesOfFileRead = fileInputStream.read(buffer)) != -1) {
                                                        out.write(buffer, 0, bytesOfFileRead);
                                                        out.flush();
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void addParam(Map<String, String> params, String key, String value) {
        String existingValue = params.get(key);
        if (existingValue == null) {
            params.put(key, value);
        } else {
            params.put(key, existingValue + "," + value);
        }
    }
    
    // Check if the URL matches a path pattern
    private static boolean doesUrlMatchPathPattern(String URL, String pathPattern, Map<String, String> pathParams) {
        String [] urlParts = URL.split("/");
        String [] pathPatternParts = pathPattern.split("/");
        
        if (urlParts.length != pathPatternParts.length) {
            return false;
        }

        for (int i = 0; i < urlParts.length; i++) {
            String urlPiece = urlParts[i];
            String pathPatternPiece = pathPatternParts[i];
            if (!urlPiece.equals(pathPatternPiece) && !pathPatternPiece.startsWith(":")) {
                return false;
            }

            if (pathPatternPiece.startsWith(":")) {
                pathParams.put(pathPatternPiece.substring(1), urlPiece);
            }
        }

        return true;
    }
    
    // Parse Host name in Header
    private static String parseHost(String requestHeader){
        String [] lines = requestHeader.split("\r\n");
        for (String line : lines) {
            if (line.toLowerCase().startsWith("host:")) {
                return line.substring("host:".length()).trim();
            }
        }
        return null;
    }
        
    // Parse Content-Length in Header
    private static int parseContentLength(String requestHeader){
        String [] lines = requestHeader.split("\r\n");
        for (String line : lines) {
            if (line.toLowerCase().startsWith("content-length:")) {
                return Integer.parseInt(line.substring("content-length:".length()).trim());
            }
        }
        return 0;
    }

    // Parse Content-Type in Header
    private static String parseContentTypeInRequest(String requestHeader){
        String [] lines = requestHeader.split("\r\n");
        for (String line : lines) {
            if (line.toLowerCase().startsWith("content-type:")) {
                return line.substring("content-type:".length()).trim();
            }
        }
        return null;
    }

    // Function to parse "If-Modified-Since" header date
    private static Date parseIfModifiedSinceDate(String requestHeader) throws ParseException {
        Date ifModifiedSinceDate = null;
        String[] lines = requestHeader.split("\r\n");
        for (String line : lines) {
            if (line.toLowerCase().startsWith("if-modified-since:")) {
                String dateString = line.substring("if-modified-since:".length()).trim();
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss z", Locale.US);
                LocalDateTime localDateTime = LocalDateTime.parse(dateString, formatter);
                ifModifiedSinceDate = Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
            }
        }
        return ifModifiedSinceDate;
    }

    // Function to fetch Content Type of the Requested File
    private static String parseContentType(String requestedFilePath) {
        String contentType = "application/octet-stream";
        if (requestedFilePath.toLowerCase().endsWith(".txt")){
            contentType = "text/plain";
        } else if (requestedFilePath.toLowerCase().endsWith(".jpg") || 
        requestedFilePath.toLowerCase().endsWith(".jpeg")){
            contentType = "image/jpeg";
        } else if (requestedFilePath.toLowerCase().endsWith(".html")){
            contentType = "text/html";
        }
        return contentType;
    }

    // Function to fetch Range Request of the Requested File
    private static String [] parseRangeRequest(String requestHeader, File requestedFile, long requestedFileLength) {
        String lowerByte = "0";
        String upperByte = Long.toString(requestedFileLength - 1);
        String[] lines = requestHeader.split(("\r\n"));
        for (String line : lines) {
            if (line.toLowerCase().startsWith("range:")) {
                String rangeStringWithUnits = line.substring("range:".length()).trim(); // bytes=25-100 or bytes=-100 or bytes=100-
                String[] rangeStringParts = rangeStringWithUnits.split("=");
                String rangeUnit = rangeStringParts[0].trim(); 
                String rangeString = rangeStringParts[1].trim(); // 25-100 or -100 or 100-

                if (!rangeUnit.trim().toLowerCase().equals("bytes")) {
                    // if unit != bytes, ignore Range field
                    String[] requestedRange = {"invalid", lowerByte, upperByte};
                    return requestedRange;
                }

                String[] rangeStringArray = rangeString.split("-");
                if (rangeStringArray.length > 0) {
                    if (!rangeStringArray[0].trim().isEmpty()) {
                        lowerByte = rangeStringArray[0].trim();
                    }

                    if (rangeStringArray.length > 1 && !rangeStringArray[1].trim().isEmpty()) {
                        upperByte = rangeStringArray[1].trim();
                    }

                    if ((Long.parseLong(upperByte)) >= requestedFileLength || 
                    (Long.parseLong(upperByte) < Long.parseLong(lowerByte)) ||
                    (Long.parseLong(upperByte) < 0 && Long.parseLong(lowerByte) < 0)) {
                        // bad range -> ignore Range field
                        String[] requestedRange = {"invalid", lowerByte, upperByte};
                        return requestedRange;
                    } else {
                        String[] requestedRange = {"valid", lowerByte, upperByte};
                        return requestedRange;
                    }
                }
            }
        }
        String[] requestedRange = {"invalid", lowerByte, upperByte}; // ignore Range field
        return requestedRange;
    }

    // Parse Session ID in Header
    private static String parseSessionId(String requestHeader){
        String [] lines = requestHeader.split("\r\n");
        for (String line : lines) {
            if (line.toLowerCase().startsWith("cookie:")) {
                String[] cookieParts = line.substring("cookie:".length()).trim().split(";");
                for (String cookie : cookieParts) {
                    String[] parts = cookie.split("=");
                    if (parts.length == 2 && parts[0].trim().equalsIgnoreCase("sessionid")) {
                        return parts[1].trim();
                    }
                }
            }
        }
        return null;
    }

    // Set Session ID for a request by parsing or creating new session object
    private static SessionImpl getSession(String requestHeader, Session currentSession, String sessionIdGenerated, RequestImpl request, Server serverInstance) {
        try {
            // Check if Request contains a session ID / cookie header
            String sessionId = parseSessionId(requestHeader);
            long currentTime = System.currentTimeMillis();
            if (sessionId != null && currentSession != null) {
                if (sessionIdGenerated != null && !sessionIdGenerated.equals(sessionId)) {
                    if(request.isSessionCalled()) {
                        // System.out.println("Request includes cookie: header, no such session, application does call session().");
                        // Maybe the session has expired or the client tried to hack its cookie store? 
                        // Proceed as if there was no Cookie: header - i.e., create a fresh session object and a fresh session ID, and send back a Set-Cookie: header.
                        Session session = currentSession;
                        return (SessionImpl) session;
                    }
                } else {
                    SessionImpl session = (SessionImpl) currentSession;
                    long lastAccessedTime = session.lastAccessedTime();
                    if (currentTime - lastAccessedTime > session.getMaxActiveInterval() * 1000) {
                        // System.out.println("Request includes cookie: header, session exists but has expired.");
                        // Proceed as in the 'no such session' cases - expired sessions are ignored even if your cleanup mechanism hasn't collected them yet.
                        session.invalidate();
                        session = serverInstance.sessionNew();
                    } else {
                        if (request.isSessionCalled()) {
                            // System.out.println("Request includes cookie: header, session exists and has not yet expired, application does call session().");
                            // Return the existing session object. 
                            // This is the common case for applications that do use sessions.
                            session.setSessionStateAsOld();
                            session.setLastAccessedTime(System.currentTimeMillis());
                        } else {
                            // System.out.println("Request includes cookie: header, session exists and has not yet expired, application does not call session().");
                            // Nothing special. Your server's response should not contain a Set-Cookie: header. 
                            // Maybe the application is rendering a page that doesn't depend on session state?
                            return null;
                        }
                    }
                    return session;
                }
            } else if (sessionId != null && currentSession==null) {
                // System.out.println("Request includes cookie: header, no such session, application does not call session().");
                // Just ignore the header. Maybe the server was restarted? 
                // In any case, there is no session and nobody is asking for it, so there is nothing that can, or needs to be, done.
                return null;
            } else {
                if (request.isSessionCalled()) {
                    // System.out.println("No Cookie: header, application does call session().");
                    // You should create a fresh session object and a fresh session ID, associate the two in your server's internal data structure, 
                    // and send back a Set-Cookie: header with the new session ID. Basically, this would happen when a new user arrives.
                    Session session = currentSession;
                    return (SessionImpl) session;
                } else {
                    // System.out.println("No Cookie: header, application doesn't call session().");
                    // Your server's response should not contain a Set-Cookie: header. This is the common case for applications that do not use sessions.
                    return null;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    // Method to send error response
    private static void sendErrorResponse(int statusCode, String statusText, String responseBody, 
    Socket clientSocket, OutputStream out) throws IOException {
        String formattedDateTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        String response = String.format("HTTP/1.1 %d %s\r\nServer: cis5550.webserver\r\nContent-Length: %d"+
        "\r\nContent-Type: text/plain\r\nDate: %s\r\n\r\n%s", statusCode, statusText, responseBody.length(), 
        formattedDateTime, responseBody);
        out.write(response.getBytes());
    }
}
