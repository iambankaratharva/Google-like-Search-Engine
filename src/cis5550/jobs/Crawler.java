package cis5550.jobs;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.Iterator;

import cis5550.flame.FlameContext;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;
import cis5550.tools.URLParser;


public class Crawler {
    public static void run(FlameContext flameContext, String args[]) throws Exception {

        if (args.length < 1) {
            flameContext.output("\nOne seed URL required.");
            return;
        }
        flameContext.output("\nStarting the crawler with seed URL: " + args[0]);
        for (String arg : args) {
            flameContext.output("\narg: " + arg);
        }

        Set<String> validWords = Files.lines(Paths.get("src/cis5550/jobs/words.txt"))
                            .collect(Collectors.toCollection(HashSet::new));
        String coordAddr = flameContext.getKVS().getCoordinator();

        /*
         * Loading the urls from previous iterations (if any)
         */
        List<String> list = new ArrayList<>();
        list.add(args[0]);
        try{
            KVSClient kvsClient = new KVSClient(coordAddr);
            Iterator<Row> iterator = kvsClient.scan("pt-url");
            while (iterator != null && iterator.hasNext()) {
                Row r = iterator.next();
                String url = "";
                for(String col: r.columns()) {
                    url = url + r.get(col);
                }
                list.add(url);
            }
            System.out.println("URL Queue has " + list.size() + " urls\n");
        } catch (FileNotFoundException foe) {
            flameContext.output("pt-URL was not found!");
        }  catch (IOException e) {
            flameContext.output("IO error while loading previous URLs from the pt-url table" + e);
        }

        FlameRDD urlQueue = flameContext.parallelize(list);
        flameContext.output("\nStarting flatmap()");
        System.out.println("\nStarting flatmap()");
        long startTime = System.currentTimeMillis();


        int maxQueueSize = 1000;

        /*
         * We want to limit the number of pages allowed to be crawled per run of crawler.
         * Hence, the limit is going to be set to around 200 pages per non-standard host.
         * Standard hosts are declared below
         */
        final Set<String> standardHosts = new HashSet<>();
        standardHosts.add("wikipedia"); standardHosts.add("reddit"); standardHosts.add("medium"); standardHosts.add("bbc");
        standardHosts.add("cnn"); standardHosts.add("nytimes"); standardHosts.add("stackoverflow"); standardHosts.add("imdb");
        standardHosts.add("allrecipes"); standardHosts.add("epicurious"); standardHosts.add("quora"); standardHosts.add("researchgate");
        standardHosts.add("scholar.google.com"); standardHosts.add("arxiv.org"); standardHosts.add("forbes"); standardHosts.add("healthline");
        standardHosts.add("webmd"); standardHosts.add("bloomberg"); standardHosts.add("tripadvisor"); standardHosts.add("sciencedirect");
        standardHosts.add("theguardian"); standardHosts.add("washingtonpost"); standardHosts.add("cnet"); standardHosts.add("techcrunch");
        // Setting the limit of standard hosts to 25000
        final int standardHostLimit = 25000;

        flameContext.setConcurrencyLevel(5);

        while (urlQueue.count() > 0) {
            FlameRDD temporaryRDD = urlQueue.flatMap((String urlString) -> {
                try {

                    /*
                     * We want the crawler to explore different content and not keep crawling the same kind of websites.
                     * Hence, there is a 90% chance that the URL will be crawled in this iteration of crawling.
                     * Note: - Decided to not implement it because it makes the crawler slow.
                     */
                    Random rand = new Random(System.currentTimeMillis());
                    if(rand.nextDouble() < 0.1) {
                        // Do not crawl the page in this round
                        return Collections.singletonList(urlString);
                    }

                    String urlStringParts[] = URLParser.parseURL(urlString);
                    if (urlStringParts[2] == null) { // Seed URL initially does not have port.
                        String protocol = urlStringParts[0];
                        String path = urlStringParts[3];
                        if (!path.startsWith("/")) {
                            path = "/" + path;
                        }
                        String port = (protocol.equalsIgnoreCase("https") ? "443" : "80");
                        urlString = protocol + "://" + urlStringParts[1] + ":" + port + path;
                    }

                    // creating the kvsClient
                    KVSClient kvsClient = new KVSClient(coordAddr);
                    String rowHash = Hasher.hash(urlString);

                    /*
                     * To control the depth of crawler for any host, setting the maximum to 200 pages for non-standard hosts.
                     * Standard hosts have the maximum set by the standardHostLimit constant.
                     */
                    Integer countForTimesCrawled = null;
                    String host = urlStringParts[1];
                    String hostHash = Hasher.hash(host);
                    try{
                        byte[] numURLsCrawled = kvsClient.get("hostCount", hostHash, "count");
                        if(numURLsCrawled == null) {
                            // this host is getting crawled the first time
                            kvsClient.put("hostCount", hostHash, "count", "0");
                            countForTimesCrawled = 0;
                        } else {
                            // checking the limit
                            countForTimesCrawled = Integer.parseInt(new String(numURLsCrawled));
                            int allowedLimit = 200;
                            for(String s: standardHosts) {
                                if(host.indexOf(s)!=-1) {
                                    allowedLimit = standardHostLimit;
                                    break;
                                }
                            }
                            // if the host has reached the limit, then return
                            if(countForTimesCrawled >= allowedLimit) {
                                return new ArrayList<>();
                            }
                        }
                    } catch (IOException ioe) {
                        System.out.println("IOException in reading hostCount table in Crawler.");
                        return new ArrayList<>();
                    } catch (NumberFormatException nfe) {
                        System.out.println("NumberFormatException in Crawler when trying to parse numURLsCrawled");
                        return new ArrayList<>();
                    }

                    if (kvsClient.get("pt-crawl", rowHash, "contentType") == null) {
                        HttpURLConnection connection = null;
                        URL url = new URI(urlString).toURL();
                        connection = (HttpURLConnection) url.openConnection();
                        connection.setConnectTimeout(5000); // 5 seconds connect timeout
                        connection.setReadTimeout(5000);    // 5 seconds read timeout
                        connection.setInstanceFollowRedirects(false);

                        String urlHost = url.getHost();
                        String urlPort = String.valueOf(url.getPort());
                        String urlHostHash = Hasher.hash(urlHost);
                        if (urlHost == null || urlHost.isBlank()) {
                            return new ArrayList<>();
                        }

                        // Handle HTTP 429 Too Many Requests
                        int responseCode = connection.getResponseCode();
                        if (responseCode == 429) {
                            // System.out.println("429 response code received");
                            String retryAfter = connection.getHeaderField("Retry-After");
                            if (retryAfter != null) {
                                int waitTime = Integer.parseInt(retryAfter);
                                long futureTime = System.currentTimeMillis() + (waitTime * 1000);
                                // System.out.println("429 Too Many Requests. Adjusting last accessed time to future after " + waitTime + " seconds.");
                                kvsClient.put("hosts", urlHostHash, "lastAccessedTime", String.valueOf(futureTime));
                                return Collections.singletonList(urlString);
                            }
                        }

                        // Robot Exclusion Protocol
                        byte[] robotsContent = kvsClient.get("hosts", urlHostHash, "robots.txt");
                        if (robotsContent == null) { // Crawler is visiting this host for first time
                            try {
                                // System.out.println("Getting robots.txt for " + urlHost);
                                url = new URI(url.getProtocol() + "://" + urlHost + ":" + urlPort + "/robots.txt").toURL();
                                connection = (HttpURLConnection) url.openConnection();
                                connection.setConnectTimeout(5000); // 5 seconds connect timeout
                                connection.setReadTimeout(5000);    // 5 seconds read timeout
                                connection.setRequestMethod("GET");
                                connection.setRequestProperty("User-Agent", "cis5550-crawler");
                                connection.connect();
                                if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
                                    robotsContent = connection.getInputStream().readAllBytes();
                                    kvsClient.put("hosts", urlHostHash, "robots.txt", robotsContent);
                                } else {
                                    robotsContent = "NaN".getBytes();
                                    kvsClient.put("hosts", urlHostHash, "robots.txt", "NaN");
                                }
                            } catch (Exception e) { // in case of error, assume no restriction
                                System.err.println("Error in getting robotsContent from robots.txt for " + url);
                                e.printStackTrace();
                                kvsClient.put("hosts", urlHostHash, "robots.txt", "NaN");
                                robotsContent = "NaN".getBytes();
                            }
                        }

                        String robotsContentStr = new String(robotsContent);
                        double crawlDelay = parseCrawlDelay(robotsContentStr);

                        if (checkLastAccessedValid(urlString, urlHostHash, kvsClient, crawlDelay)) {
                            boolean isAllowed = RobotExclusionProtocol.isUrlAllowed(robotsContentStr, url.getPath().toString(),"cis5550-crawler");
                            if (isAllowed) {
                                List<String> extractedUrls = new ArrayList<>();
                                // System.out.println("Sending HEAD request to " + urlString);

                                // HEAD
                                url = new URI(urlString).toURL();
                                connection = (HttpURLConnection) url.openConnection();
                                // Set the connect and read timeouts (in milliseconds)
                                connection.setConnectTimeout(5000); // 5 seconds connect timeout
                                connection.setReadTimeout(5000);    // 5 seconds read timeout
                                connection.setInstanceFollowRedirects(false);
                                connection.setRequestMethod("HEAD");
                                connection.setRequestProperty("User-Agent", "cis5550-crawler");
                                connection.connect();

                                // Update last accessed time for HEAD
                                kvsClient.put("hosts", urlHostHash, "lastAccessedTime", String.valueOf(System.currentTimeMillis()));

                                Row row = kvsClient.getRow("pt-crawl", rowHash);
                                if (row == null) {
                                    row = new Row(rowHash);
                                }
                                String connectionResponseCode = String.valueOf(connection.getResponseCode());
                                String contentType = connection.getContentType();
                                row.put("responseCode", connectionResponseCode.getBytes());
                                if (contentType == null) {
                                    kvsClient.put("pt-crawl", rowHash, "responseCode", connectionResponseCode.getBytes());
                                    return new ArrayList<String>();
                                }
                                row.put("contentType", connection.getContentType());
                                row.put("url", urlString);
                                // for (String column : row.columns()) {
                                //     kvsClient.put("pt-crawl", rowHash, column, row.get(column));
                                // }
                                kvsClient.putRow("pt-crawl", row);

                                /*
                                 * We want to not allow redirects to go beyond 3 to avoid the malicious sites.
                                 * We maintain a counter to check after how many redirects we reach the current URL.
                                 * We use an in-memory KVS table for this.
                                 */
                                if (VALUES.contains(connectionResponseCode)) {

                                    try{
                                        // Check the number of redirects made to reach this url, if any.
                                        byte[] numRedirectsByteArray = kvsClient.get("redirectTable", rowHash, "count");
                                        int previousRedirects;
                                        if(numRedirectsByteArray == null) {
                                            // no previous redirect to reach this url
                                            previousRedirects = 0;
                                        } else {
                                            // getting the previous redirects
                                            previousRedirects = Integer.parseInt(new String(numRedirectsByteArray));
                                        }
                                        // Not Crawling this url if this is reached after 3 redirects
                                        if(previousRedirects > 3) {
                                            return new ArrayList<>();
                                        }

                                        List<String> newUrl = new ArrayList<>();
                                        newUrl.add(connection.getHeaderField("Location"));
                                        String redirectedUrl = normalizeUrls(newUrl, urlString, blacklistPatternsArr).get(0);
                                        if (!isUrlBlackListed(redirectedUrl, blacklistPatternsArr)) {
                                            extractedUrls.addAll(Collections.singletonList(redirectedUrl));
                                            // The next URL will be reached after previousRedirects + 1
                                            try {
                                                kvsClient.put("redirectTable", Hasher.hash(redirectedUrl), "count", "" + (previousRedirects + 1));
                                            } catch (IOException ioe) {
                                                System.out.println("IOException in writing to redirectTable table in Crawler.");
                                                return new ArrayList<>();
                                            }
                                        }
                                        return extractedUrls;
                                    } catch (IOException ioe) {
                                        System.out.println("IOException in reading redirectTable table in Crawler.");
                                        return new ArrayList<>();
                                    } catch (NumberFormatException nfe) {
                                        System.out.println("NumberFormatException in Crawler when trying to parse numRedirectsByteArray");
                                        return new ArrayList<>();
                                    }
                                }

                                if (connectionResponseCode.equalsIgnoreCase("200") && (contentType != null && contentType.toLowerCase().startsWith("text/html"))) {
                                    // GET
                                    connection = (HttpURLConnection) url.openConnection();
                                    // Set the connect and read timeouts (in milliseconds)
                                    connection.setConnectTimeout(5000); // 5 seconds connect timeout
                                    connection.setReadTimeout(5000);    // 5 seconds read timeout
                                    connection.setRequestMethod("GET");
                                    connection.setRequestProperty("User-Agent", "cis5550-crawler");
                                    connection.connect();
                                    // Update last accessed time for GET
                                    kvsClient.put("hosts", urlHostHash, "lastAccessedTime", String.valueOf(System.currentTimeMillis()));

                                    final int bufferSize = 524288; // 128KB: 512 * 1024
                                    BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                                    char[] buffer = new char[bufferSize];
                                    int bytesRead;
                                    StringBuilder response = new StringBuilder(bufferSize);
                                    int maxLength = 2000000;
                                    while ((bytesRead = in.read(buffer, 0, buffer.length)) != -1) {
                                        // contentLength - response.length() gives the number of characters that are allowed to be read.
                                        // if bytesRead > contentLength - response.length(), then prune the rest.
                                        if(bytesRead <= maxLength - response.length()) {
                                            response.append(buffer, 0, bytesRead);
                                        } else {
                                            response.append(buffer, 0, maxLength - response.length());
                                            break;
                                        }
                                    }
                                    in.close();

                                    String htmlContent = response.toString();
                                    htmlContent = htmlContent.replaceAll("<script[^>]*>[\\s\\S]*?</script>", "");
                                    htmlContent = htmlContent.replaceAll("<style[^>]>[\\s\\S]?</style>", "");
                                    htmlContent = htmlContent.replaceAll("\\s{2,}", " ");

                                    //check for pages written in english
                                    if(!isEnglishLang(htmlContent)) {
                                        return new ArrayList<String>();
                                    }

                                    if (checkForDuplicateContent(urlString, htmlContent, rowHash, kvsClient)) {
                                        return new ArrayList<String>();
                                    }

                                    // row.put("page", htmlContent.getBytes());
                                    // for (String column : row.columns()) {
                                    //    kvsClient.put("pt-crawl", rowHash, column, row.get(column));
                                    // }
                                    // kvsClient.putRow("pt-crawl", row);
                                    extractedUrls.addAll(extractUrls(htmlContent, urlString, blacklistPatternsArr, robotsContentStr, kvsClient));
                                    
                                    htmlContent = htmlContent.replaceAll("<[^>]*>", " ").replaceAll("\\p{Punct}", " ").toLowerCase();
                                    String[] pageContentFiltered = Arrays.stream(htmlContent.split("\\s+|(?<=\\d)(?=\\D)|(?<=\\D)(?=\\d)"))
                                                                        .filter(word -> !stopwords.contains(word))
                                                                        .toArray(String[]::new);
                                    String[] validPageContent = Arrays.stream(pageContentFiltered)
                                                                    .filter(word -> validWords.contains(word))
                                                                    .toArray(String[]::new);
                                    htmlContent = String.join(" ", validPageContent);
                                    StringBuilder sb = new StringBuilder();
                                    for(String u: extractedUrls) {
                                        sb.append(" <a href=\""+u+"\"></a>");
                                    }
                                    htmlContent += sb.toString();
                                    kvsClient.put("pt-crawl", rowHash, "length", String.valueOf(htmlContent.length()));
                                    kvsClient.put("pt-crawl", rowHash, "page", htmlContent);
                                    /*
                                     * Increasing the count that this host has been crawled for.
                                     */
                                    countForTimesCrawled++;
                                    kvsClient.put("hostCount", hostHash, "count", countForTimesCrawled.toString());

                                    System.out.println("Crawled " + urlString + " " + rowHash + " in queue " + extractedUrls.size());
                                    return extractedUrls;
                                }
                            }
                        } else {
                            return Collections.singletonList(urlString);
                        }
                    } else {
                        // System.out.println("Already crawled " + urlString);
                    }
                }
                catch (java.net.SocketTimeoutException e) {
                    System.out.println("Timeout occurred for URL: " + urlString);
                    // Handle the timeout specifically, e.g., log it, skip this URL, or retry later
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
                return new ArrayList<String>();
            });

            System.out.println("Length of queue: " + urlQueue.count());
            /*
             * Deleting the stale table to save space
             */
            try{
                urlQueue.destroy();
            } catch(Exception e) {
                System.out.println("Error while destroying some intermediate RDD");
            }

            urlQueue = temporaryRDD;

            // Check if the URL queue size exceeds the maximum allowed size
            if (urlQueue.count() > maxQueueSize) {
                // Calculate the sampling probability to reduce the queue size to the maximum limit
                // double sampleProbability = (double) maxQueueSize / urlQueue.count();
                
                Vector<String> vc = urlQueue.take(maxQueueSize / 10);

                List<String> ls = new ArrayList<>();
                for(String s : vc) {
                    ls.add(s);
                }
                // Sample the URL queue to reduce its size

                temporaryRDD = flameContext.parallelize(ls);

                System.out.println("New URL Queue size after pruning: " + temporaryRDD.count());

                // Attempt to release resources by destroying the old URL queue
                try {
                    urlQueue.destroy();
                } catch (Exception e) {
                    System.out.println("Error while destroying the URL queue: " + e.getMessage());
                }

                // Reassign the reduced queue back to urlQueue
                urlQueue = temporaryRDD;
            }

            // deleting the previous pt-url table
            try{
                KVSClient kvsClient = new KVSClient(coordAddr);
                kvsClient.delete("pt-url");
                Thread.sleep(1000);
            } catch (IOException ioe) {
                flameContext.output("Error while deleting pt-url table\n");
            }

            flameContext.output("performing kvs put into pt-url");

            // Creating the new pt-url table
            try{
                temporaryRDD = urlQueue.flatMap((urlString) -> {
                    KVSClient kvsClient = new KVSClient(coordAddr);
                    kvsClient.put("pt-url", Hasher.hash(urlString), "value", urlString);
                    return new ArrayList<>();
                });
                temporaryRDD.destroy();
            } catch (Exception e) {
                flameContext.output("Error while creating the pt-url table: " + e);
            }
        }

        flameContext.output("\nExecuted flatmap()");
        flameContext.output("Crawl Time: " + (System.currentTimeMillis() - startTime)/1000 + " seconds\n");
    }

    private static boolean isEnglishLang(String htmlContent) {
        // <html lang="xx">
        Pattern langPattern = Pattern.compile("<html\\s+(?:[^>]*?\\s+)?lang=\"([^\"]+)\"", Pattern.CASE_INSENSITIVE);
        Matcher matcher = langPattern.matcher(htmlContent);

        if (matcher.find()) {
            String langCode = matcher.group(1).toLowerCase();
            return langCode.equals("en") || langCode.startsWith("en-");
        }
        return true;
    }

    private static String capSubdirectoryDepth(String path) {
        // Normalize the path to remove any '../' or './'
        path = path.replaceAll("(\\.\\./|\\./)", "");

        String[] segments = path.split("/");
        int depthCount = 0;
        for (String segment : segments) {
            if (!segment.isEmpty() && !segment.equals("..")) {
                depthCount++;
            }
        }
        if (depthCount > 5) {
            // System.out.println("Url depth exceeded 5 : " + path);
            return null;
        }
        return path;
    }

    private static boolean checkForDuplicateContent(String urlString, String htmlContent, String rowHash, KVSClient kvsClient) throws IOException {
        /*
         * Making the content seen table persistent as we might need to restart workers
         */
        Row duplicateContent = kvsClient.getRow("pt-contentSeen", Hasher.hash(htmlContent));
        if (duplicateContent == null) { // no duplicate content
            kvsClient.put("pt-contentSeen", Hasher.hash(htmlContent), "url", urlString);
            return false;
        }
        // row.put("canonicalURL", duplicateContent.get("url"));
        kvsClient.put("pt-crawl", rowHash, "canonicalURL", duplicateContent.get("url"));
        // System.out.println("Duplicate page detected: " + urlString);
        return true;
    }

    private static double parseCrawlDelay(String robotsContentStr) {
        int crawlDelayIndex = robotsContentStr.toLowerCase().indexOf("crawl-delay:");
        double crawlDelay = 1000; // default 1 seconds
        if (crawlDelayIndex >= 0) {
            String crawlDelayStr = robotsContentStr.substring(crawlDelayIndex + 12).split("\n")[0];
            crawlDelay = Double.parseDouble(crawlDelayStr) * 1000;
        }
        return crawlDelay;
    }

    private static boolean checkLastAccessedValid(String urlString, String urlHostHash, KVSClient kvsClient, double crawlDelay) throws IOException {
        byte[] lastAccessedTime = kvsClient.get("hosts", urlHostHash, "lastAccessedTime");
        long prevCrawledTime = -999;
        if (lastAccessedTime != null) {
            prevCrawledTime = Long.parseLong(new String(lastAccessedTime));
        } else {
            return true;
        }
        if (prevCrawledTime > 0 && ((System.currentTimeMillis() - prevCrawledTime) < crawlDelay)) {
            return false;
        }
        return true;
    }

    private static Set<String> extractUrls(String htmlContent, String urlString, Set<String> blacklistPatternsArr, String robotsContentStr, KVSClient kvsClient) {
        Set<String> extractedUrlsSet = new HashSet<String>();
        // Regular expression to find <a> tags and extract href values.
        // It captures href attribute values regardless of the position in the tag and ignores case sensitivity.
        // Ref: https://stackoverflow.com/questions/15926142/regular-expression-for-finding-href-value-of-a-a-link
        Pattern pattern = Pattern.compile("<a\\s+(?:[^>]*?\\s+)?href=\"([^\"]*)\"[^>]*>(.*?)</a>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher matcher = pattern.matcher(htmlContent);

        while (matcher.find()) {
            try {
                String url = matcher.group(1);
                List<String> normalizedUrl = normalizeUrls(Collections.singletonList(url), urlString, blacklistPatternsArr);
                if (!normalizedUrl.isEmpty()) {
                    String normalizedUrlStr = normalizedUrl.get(0);
                    String normalizedPath = new URI(normalizedUrlStr).toURL().getPath();
                    boolean isAllowed = RobotExclusionProtocol.isUrlAllowed(robotsContentStr, normalizedPath,"cis5550-crawler");
                    if (isAllowed) {
                        String normalizedUrlStrHashed = Hasher.hash(normalizedUrlStr);
                        if (!isUrlBlackListed(normalizedUrlStr, blacklistPatternsArr)) {
                            if (kvsClient.get("pt-crawl", normalizedUrlStrHashed, "contentType") == null) {
                                /*
                                 * For Authenticity we only want to crawl HTTPS websites
                                 * If the website is not HTTPS, then skip.
                                 */
                                String urlStringParts[] = URLParser.parseURL(urlString);
                                if(urlStringParts[0].trim().toLowerCase().equals("https")) {
                                    extractedUrlsSet.add(normalizedUrlStr);
                                }
                            }
                            // String anchorTextOfUrl = matcher.group(2).replaceAll("\\s+", " ").trim();
                            // StringBuilder anchorText = new StringBuilder();
                            // if (anchorTextOfUrl != null && !anchorTextOfUrl.isBlank()) {
                            //     byte[] prevAnchorText = kvsClient.get("pt-crawl", normalizedUrlStrHashed, "anchor:"+urlString);
                            //     if (prevAnchorText != null) {
                            //         anchorText.append(new String(prevAnchorText)).append(" ").append(anchorTextOfUrl);
                            //     } else {
                            //         anchorText.append(anchorTextOfUrl);
                            //     }
                            //     String newRow = "anchor:"+urlString;
                            //     kvsClient.put("pt-crawl", normalizedUrlStrHashed, newRow, anchorText.toString());
                            // }
                        }
                    }
                } else {
                    // System.out.println("normalizedUrl is empty");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        /*
         * We will only select max_size URLs max to add to the queue to control growth of the queue.
         */
        int max_size = 30;
        if(extractedUrlsSet.size() > max_size) {
            // add the elements from set to list
            List<String> extractedURLsList = new ArrayList<>();
            for(String s: extractedUrlsSet) extractedURLsList.add(s);

            // Shuffle the list
            Collections.shuffle(extractedURLsList, new Random(System.currentTimeMillis()));

            // Select the top elements
            extractedUrlsSet = new HashSet<>();
            for(int i = 0; i < max_size; i++) {
                extractedUrlsSet.add(extractedURLsList.get(i));
            }
        }

        return extractedUrlsSet;
    }

    private static List<String> normalizeUrls(List<String> urls, String baseUrl, Set<String> blacklistPatternsArr) {
        List<String> normalizedUrls = new ArrayList<String>();
        String[] baseParts = URLParser.parseURL(baseUrl);
        String baseProtocol = baseParts[0];
        String baseHost = baseParts[1];
        String basePort = baseParts[2];
        String basePath = baseParts[3];

        for (String url : urls) {
            try {
                basePath = baseParts[3];
                String[] urlParts = URLParser.parseURL(url);
                String protocol = (urlParts[0] == null || urlParts[0].isEmpty()) ? baseProtocol : urlParts[0];
                String host = (urlParts[1] == null || urlParts[1].isEmpty()) ? baseHost : urlParts[1];
                String port = (urlParts[2] == null || urlParts[2].isEmpty()) ? basePort : urlParts[2];
                String path = (urlParts[3] == null || urlParts[3].isEmpty()) ? basePath : urlParts[3];

                path = capSubdirectoryDepth(path);
                if (path == null) {
                    continue;  // If path is null --> subdirectory depth exceeded
                }

                if (!"https".equalsIgnoreCase(protocol) && !"http".equalsIgnoreCase(protocol)) {
                    // System.out.println("Protocol not https / http: " + protocol);
                    return normalizedUrls;
                }
                String normalizedPath = path.toLowerCase();
                for (String extension : INVALID_EXTENSIONS) {
                    if (normalizedPath.endsWith(extension)) {
                        // System.out.println("Invalid extension: " + path);
                        return normalizedUrls;
                    }
                }

                // Cut off anything after the # in the path
                int hashIndex = path.indexOf("#");
                if (hashIndex != -1) {
                    path = path.substring(0, hashIndex);
                }

                path = path.replace(" ", "%20");

                if (path.startsWith("../")) {
                    basePath = basePath.substring(0, basePath.lastIndexOf('/', basePath.length() - 2) + 1);
                    while (path.startsWith("../")) {
                        // Find the index of the last directory separator in 'basePath', excluding the trailing slash.
                        int slashIndex = basePath.lastIndexOf('/', basePath.length() - 2);
                        if (slashIndex != -1) { // Keep the base path up to the last directory separator
                            basePath = basePath.substring(0, slashIndex + 1);
                        } else {
                            break;
                        }
                        // Remove '../'
                        path = path.substring(3);
                        if (!path.contains("../")) {
                            path = basePath + path;
                        }
                    }
                }

                // Reconstruct the normalized URL
                if (!path.startsWith("/")) {
                    path = basePath.substring(0, basePath.lastIndexOf('/')) + "/" + path;
                }
                String normalizedUrlStr = protocol + "://" + host + ":" + port + path;
                normalizedUrls.add(normalizedUrlStr.trim());
            } catch (Exception e) {
                e.printStackTrace();
                System.err.println("Error normalizing URL: " + url);
            }
        }
        return normalizedUrls;
    }

    private static boolean isUrlBlackListed(String normalizedUrlStr, Set<String> blacklistPatternsArr) {
        if (blacklistPatternsArr != null) {
            for (String blacklistPattern : blacklistPatternsArr) {
                Pattern pattern = Pattern.compile(blacklistPattern);
                if (pattern.matcher(normalizedUrlStr).find()) {
                    // System.out.println("Blacklisted URL encountered: " + normalizedUrlStr);
                    return true;
                }
            }
        }
        return false;
    }

    private static final Set<String> VALUES = new HashSet<String>();
    static Set<String> blacklistPatternsArr = new HashSet<String>();
    static {
        VALUES.add("301");
        VALUES.add("302");
        VALUES.add("303");
        VALUES.add("307");
        VALUES.add("308");
    }
    private static final Set<String> INVALID_EXTENSIONS = new HashSet<>();
    static {
        INVALID_EXTENSIONS.add(".jpg");
        INVALID_EXTENSIONS.add(".jpeg");
        INVALID_EXTENSIONS.add(".gif");
        INVALID_EXTENSIONS.add(".png");
        INVALID_EXTENSIONS.add(".txt");
    }
    static {
        blacklistPatternsArr.add("mailto");
        blacklistPatternsArr.add("\\+tel");

        // Social media platforms
        blacklistPatternsArr.add("snapchat");
        blacklistPatternsArr.add("youtube");
        blacklistPatternsArr.add("whatsapp");
        blacklistPatternsArr.add("telegram");
        blacklistPatternsArr.add("linkedin");
        blacklistPatternsArr.add("twitter");

        // E-commerce sites
        blacklistPatternsArr.add("amazon");
        blacklistPatternsArr.add("ebay");
        blacklistPatternsArr.add("alibaba");
        blacklistPatternsArr.add("etsy");

        // Media sharing
        blacklistPatternsArr.add("flickr");
        blacklistPatternsArr.add("tumblr");

        // Utility and common services
        blacklistPatternsArr.add("wordpress");
        blacklistPatternsArr.add("paypal");
        blacklistPatternsArr.add("drive.google");
        blacklistPatternsArr.add("dropbox");

        // Professional and job-related platforms
        blacklistPatternsArr.add("glassdoor");
        blacklistPatternsArr.add("indeed");

        // Code repositories and development platforms
        blacklistPatternsArr.add("github");
        blacklistPatternsArr.add("gitlab");
        blacklistPatternsArr.add("bitbucket");

        // Miscellaneous
        blacklistPatternsArr.add("login"); // General pattern to avoid login pages
        blacklistPatternsArr.add("register"); // Avoid registration pages
        blacklistPatternsArr.add("cgi-bin"); // Avoid executing scripts or accessing the backend directly
    }

    private static Set<String> stopwords = new HashSet<>(Arrays.asList(
        "i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours",
        "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers",
        "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves",
        "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are",
        "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does",
        "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as",
        "until", "while", "of", "at", "by", "for", "with", "about", "against", "between",
        "into", "through", "during", "before", "after", "above", "below", "to", "from",
        "up", "down", "in", "out", "on", "off", "over", "under", "again", "further",
        "then", "once", "here", "there", "when", "where", "why", "how", "all", "any",
        "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor",
        "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can",
        "will", "just", "don", "should", "now"
    ));
}