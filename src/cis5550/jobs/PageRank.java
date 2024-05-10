package cis5550.jobs;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import cis5550.flame.FlameContext;
import cis5550.flame.FlamePair;
import cis5550.flame.FlamePairRDD;
import cis5550.flame.FlameRDD;
import cis5550.kvs.KVSClient;
import cis5550.tools.Hasher;
import cis5550.tools.URLParser;

public class PageRank {

    private static final Set<String> supportedProtocols = Set.of("http", "https");
    private static final Set<String> unsupportedExtensions = Set.of(".jpg", ".jpeg", ".gif", ".png", ".txt");
  
    public static List<String> normalizeUrls(List<String> urls, String baseUrl) {

        List<String> normalizedUrls = new ArrayList<>();
        String[] baseParts = URLParser.parseURL(baseUrl);
        String baseProtocol = baseParts[0];
        String baseHost = baseParts[1];
        String basePort = (baseParts[2] == null || baseParts[2].isEmpty()) ? (baseProtocol.equals("https") ? "443" : "80") : baseParts[2];
        String basePath = baseParts[3];
    
        for (String rawUrl : urls) {
            String[] rawUrlParts = URLParser.parseURL(rawUrl);
            String protocol = (rawUrlParts[0] == null || rawUrlParts[0].isEmpty()) ? baseProtocol : rawUrlParts[0];
            String host = (rawUrlParts[1] == null || rawUrlParts[1].isEmpty()) ? baseHost : rawUrlParts[1];
            String port = (rawUrlParts[2] == null || rawUrlParts[2].isEmpty()) ? (protocol.equals("https") ? "443" : "80") : rawUrlParts[2];
            String path = rawUrlParts[3];
    
            int hashIndex = path.indexOf('#');
            if (hashIndex >= 0) {
                path = path.substring(0, hashIndex);
            }
    
            if (path.startsWith("/")) {
                basePath = "";
            } else if (path.startsWith("../")) {
                while (path.startsWith("../")) {
                    path = path.substring(3);
                    basePath = basePath.substring(0, basePath.lastIndexOf("/", basePath.length() - 2) + 1);
                }
            } else if (!path.isEmpty() && !path.startsWith("#")) {
                basePath = basePath.substring(0, basePath.lastIndexOf("/") + 1);
            }
    
            String normalizedUrl = protocol + "://" + host + (port.isEmpty() ? "" : ":" + port) + basePath + path;
            normalizedUrls.add(normalizedUrl);
        }
    
        return normalizedUrls;
    } 

    public static Map<String, String> extractUrlsAndTexts(String content, String baseUrl) {

        Map<String, String> urlsAndTexts = new HashMap<>();

        Pattern pattern = Pattern.compile("<a\\s+(?:[^>]*?\\s+)?href=\"([^\"]*)\"[^>]*>(.*?)</a>", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher matcher = pattern.matcher(content);

        while (matcher.find()) {
            String url = matcher.group(1);
            String anchorText = matcher.group(2).replaceAll("\\s+", " ").trim(); 
            urlsAndTexts.merge(url, anchorText, (existingText, newText) -> existingText + " " + newText);
        }

        Map<String, String> normalizedUrlsAndTexts = new LinkedHashMap<>();

        urlsAndTexts.forEach((url, text) -> {
            List<String> normalizedUrl = normalizeUrls(Collections.singletonList(url), baseUrl);
            if (!normalizedUrl.isEmpty() && shouldIncludeURL(normalizedUrl.get(0))) {
                normalizedUrlsAndTexts.put(normalizedUrl.get(0), text);
            }

        });

        return normalizedUrlsAndTexts;
    } 

    @SuppressWarnings("deprecation")
    public static boolean shouldIncludeURL(String url) {
        try {
          URL parsedUrl = new URL(url);
          String protocol = parsedUrl.getProtocol();
          if (!supportedProtocols.contains(protocol)) {
            return false;
          }
    
          String path = parsedUrl.getPath().toLowerCase();
          for (String extension : unsupportedExtensions) {
            if (path.endsWith(extension)) {
              return false;
            }
          }
        } catch (MalformedURLException e) {
          return false;
        }
        return true;
    }
    
    public static void run(FlameContext flameContext, String[] args) throws Exception {

        if(args.length < 1) return;
        
        final Double convergenceThreshold = Double.parseDouble(args[0]);
        final Double convergencePercentage = args.length > 1 ? Double.parseDouble(args[1]) : 100.0;
        String coordinatorAddr = flameContext.getKVS().getCoordinator();

        try {

            flameContext.output("\nStarting pagerank job @ " + System.currentTimeMillis());
        
            FlameRDD rawData = flameContext.fromTable("pt-crawl", row -> {
                String url = row.get("url");
                String content = row.get("page");
                System.out.println("Getting pt-crawl data from urls: " + url);
                return url + "," + content; 
            });

            int totalUrls = rawData.count();
            System.out.println("Total docs: " + totalUrls);

            System.out.println("Starting mapToPair on Raw Data");
            FlamePairRDD stateTableRDD = rawData.mapToPair(s -> { 
                String[] parts = s.split(",", 2);
                String url = parts[0];
                String content = parts[1];
                Map<String, String> extractedUrlsMap = extractUrlsAndTexts(content, url);

                StringBuilder sb = new StringBuilder();
                for(String extractedUrl : extractedUrlsMap.keySet()) {
                    sb.append(Hasher.hash(extractedUrl)).append(",");
                }

                String allUrls = "";
                if (!sb.isEmpty()) {
                    allUrls = sb.toString().substring(0, sb.length()-1);
                }

                return new FlamePair(Hasher.hash(url), "1.0,1.0," + allUrls);
            });

            // stateTableRDD.saveAsTable("state");

            int i = 0;

            while(true) {

                System.out.println("Iteration: " + i);

                FlamePairRDD transferTableRDD = stateTableRDD.flatMapToPair(pair -> {

                    String urlHash = pair._1();
                    String[] parts = pair._2().split(",", 3);
                    Double currentRank = Double.parseDouble(parts[0]);
                    String[] links = parts[2].isEmpty() ? new String[0] : parts[2].split(",");
            
                    List<FlamePair> transfers = new ArrayList<>();

                    if(links.length == 0) {
                        transfers.add(new FlamePair(urlHash, "0.0"));
                    }
                    
                    if(links.length > 0) {
                        Double rankToTransfer = 0.85 * currentRank / links.length;
                        for (String link : links) {
                            transfers.add(new FlamePair(link, String.valueOf(rankToTransfer)));
                            if(!link.equalsIgnoreCase(urlHash)) {
                                transfers.add(new FlamePair(urlHash, "0.0"));
                            }
                        }
                    }
                    // transfers.forEach(transfer -> System.out.println("Transfer: " + transfer._1() + " gets " + transfer._2()));
            
                    return transfers;

                });

                System.out.println("Transfer table for iteration " + i + " done");

                FlamePairRDD aggregatedTable = transferTableRDD.foldByKey("0.0", (accumulator, rankValue) -> {

                    Double r1 = Double.parseDouble(accumulator);
                    Double r2  = Double.parseDouble(rankValue);
                    return String.valueOf(r1 + r2);

                });

                transferTableRDD.destroy();

                FlamePairRDD joinedRDD = aggregatedTable.join(stateTableRDD);

                System.out.println("joinedRDD table for iteration " + i + " done");

                aggregatedTable.destroy();

                FlamePairRDD updatedStateTableRDD = joinedRDD.flatMapToPair(pair -> {

                    String urlHash = pair._1();
                    String value = pair._2();

                    String[] parts = value.split(",", 4);

                    Double previousRank = Double.parseDouble(parts[1]); 
                
                    Double newCurrentRank =  Double.parseDouble(parts[0]) + 0.15;
                    
                    String rankAndUrls = newCurrentRank + "," + previousRank;
            
                    if (parts.length > 3) {
                        rankAndUrls = rankAndUrls + "," + parts[3];
                    } else {
                        rankAndUrls = rankAndUrls + ""; 
                    }

                    FlamePair updatedPair = new FlamePair(urlHash, rankAndUrls);

                    return Arrays.asList(updatedPair);

                });

                System.out.println("Updated State table for iteration " + i + "done");

                joinedRDD.destroy();

                FlameRDD finalRanks = updatedStateTableRDD.flatMap(pair -> {

                    String[] parts = pair._2().split(",");
                    Double previousRank = Double.parseDouble(parts[1]);
                    Double currentRank = Double.parseDouble(parts[0]);
                    Double diff = Math.abs(currentRank - previousRank);
                    return Collections.singletonList(String.valueOf(diff));
                });

                FlameRDD urlsMeetingCriterion = finalRanks.filter(diff -> {
                    return Double.parseDouble(diff) <= convergenceThreshold;
                });

                double meetingPercentage = ((double) urlsMeetingCriterion.count() / totalUrls) * 100;
                
                System.out.println("Meeting percentage is: " + meetingPercentage);
                
                boolean isConverged = meetingPercentage >= convergencePercentage;

                if(isConverged) {
                    stateTableRDD = updatedStateTableRDD;
                    break;
                }

                String maxRankDiff = finalRanks.fold("0.0", (acc, newValue) -> {
                    return String.valueOf(Math.max(Double.parseDouble(acc), Double.parseDouble(newValue)));
                });

                System.out.println("Max Rank diff for iteration " + i + " " + maxRankDiff);

                stateTableRDD.destroy();
                finalRanks.destroy();

                stateTableRDD = updatedStateTableRDD;

                if ((maxRankDiff != null && (Double.parseDouble(maxRankDiff) < convergenceThreshold))) {
                    break;
                } 

                i += 1;

            }

            FlamePairRDD pageRanksRDD = stateTableRDD.flatMapToPair(pair -> {
                // List<FlamePair> pageRanks = new ArrayList<>();
                String parts[] = pair._2().split(",");
                String urlHash = pair._1();
                Double rank = Double.parseDouble(parts[0]);

                KVSClient kvs = new KVSClient(coordinatorAddr);
                kvs.put("pt-pageranks", urlHash, "rank", String.valueOf(rank));
                return Collections.singletonList(new FlamePair(urlHash, String.valueOf(rank)));
            });

            // pageRanksRDD.saveAsTable("pt-pageranks");
            System.out.println("Page ranks table");

            flameContext.output("\nEnding pagerank job @ " + System.currentTimeMillis());
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
}

