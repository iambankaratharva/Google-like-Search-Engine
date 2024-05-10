package cis5550.jobs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import cis5550.flame.*;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;

public class Indexer {
    public static void run(FlameContext flameContext, String[] args) throws Exception {
        // ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
        try {
            System.out.println("\nStarting indexing job @ " + System.currentTimeMillis());
            String coordinatorAddr = flameContext.getKVS().getCoordinator();
            FlameRDD rawData = flameContext.fromTable("pt-crawl", row -> {
                String url = row.get("url");
                String content = row.get("page");
                String responseCode = row.get("responseCode");
                if (url == null || content == null || !responseCode.equals("200") || url.matches(".*\\.\\..*") || url.length() > 2048) {
                    System.out.println("Blocked Url: " + url);
                    return null;
                }
                content = content.replaceAll("<[^>]*>", " ");
                
                content = String.join(" ", content);
                                                        
                System.out.println("Thread " + Thread.currentThread().threadId() + ": Loading data @ " + "url: " + url + " content.length: " + content.length());
                return Hasher.hash(url) + "," + content + " _@_@ " + url;
            });

            int totalDocuments = rawData.count();
            System.out.println("\ntotalDocuments in pt-crawl: " + totalDocuments);

            FlamePairRDD pairRDD = rawData.mapToPair(s -> {
                String[] parts = s.split(",", 2);
                System.out.println("Thread " + Thread.currentThread().threadId() + ": Transforming data to pairs @ " + parts[0]);
                return new FlamePair(parts[0], parts[1]);
            });

            rawData.destroy();
            System.out.println("\nDone with mapToPair");

            long startTime = System.nanoTime();
            FlamePairRDD wordPairs = pairRDD.flatMapToPair(pair -> {
                try {
                    String url = pair._1();
                    String page = pair._2().split(" _@_@ ")[0];
                    String unhashedUrl = pair._2().split(" _@_@ ")[1];

                    System.out.println("Thread " + Thread.currentThread().threadId() + ": Extracting & stemming for " + url + " || " + unhashedUrl);

                    Map<String, List<Integer>> wordPositions = new HashMap<>();
                    int position = 1;
                    PorterStemmer stemmer = new PorterStemmer();
                    String[] validPageContent = page.split(" ");
                    for (String token : validPageContent) {
                        if (!token.trim().isEmpty()) {
                            stemmer.add(token.toCharArray(), token.length());
                            stemmer.stem();
                            String stemmedtoken = stemmer.toString();

                            wordPositions.computeIfAbsent(token, k -> new ArrayList<>())
                                    .add(position);
                            if (!stemmedtoken.equals(token)) {
                                wordPositions.computeIfAbsent(stemmedtoken, k -> new ArrayList<>())
                                        .add(position);
                            }
                        }
                        position++;
                    }

                    Double l2Norm = 0.0;
                    for (Map.Entry<String, List<Integer>> entry : wordPositions.entrySet()) {
                        Integer termFrequency = entry.getValue().size();
                        l2Norm += termFrequency * termFrequency;
                    }
                    l2Norm = Math.sqrt(l2Norm);

                    List<FlamePair> pairsList = new ArrayList<>();
                    for (Map.Entry<String, List<Integer>> entry : wordPositions.entrySet()) {
                        String word = entry.getKey();

                        // Generate pairs and put data in KVS
                        String positionsStr = entry.getValue().stream().map(Object::toString).collect(Collectors.joining(" "));
                        String urlWithPositions = url + " _@_@ " + positionsStr + " _@_@ " + l2Norm + " _@_@ " + unhashedUrl;
                        pairsList.add(new FlamePair(word, urlWithPositions));
                    }

                    return pairsList;
                } catch(Exception e) {
                    e.printStackTrace();
                    return null;
                }
            });
            long endTime = System.nanoTime();
            // Calculate the duration
            long duration = (endTime - startTime);  // Duration in nanoseconds
            System.out.println("Execution time in nanoseconds: " + duration);
            System.out.println("Execution time in milliseconds: " + duration / 1_000_000);

            pairRDD.destroy();

            FlamePairRDD invertedIndex = wordPairs.foldByKey("", (word, urlWithPosition) -> {
                if (word == null || word.isEmpty()) {
                    return urlWithPosition;
                }
                if (urlWithPosition == null || urlWithPosition.isEmpty()) {
                    return word;
                }
                System.out.println("Thread " + Thread.currentThread().threadId() + ": Creating inverted index");
                Set<String> urlSet = new HashSet<>(Arrays.asList(word.split("\\|")));
                urlSet.add(urlWithPosition);
                return String.join("|", urlSet);
            });

            wordPairs.destroy();

            try {
                System.out.println("Thread " + Thread.currentThread().threadId() + ": Creating pt-index table");
                invertedIndex.flatMapToPair(pair -> {
                    String word = pair._1();
                    String urls = pair._2();
                    System.out.println("Thread " + Thread.currentThread().threadId() + " " + word + " : Saving to pt-index");

                    KVSClient kvsClient = new KVSClient(coordinatorAddr);
                    byte[] urlSet = kvsClient.get("pt-index", word, "acc");
                    String urlSetStr = null;
                    if (urlSet != null) {
                        System.out.println(word + " already exists.");
                        urlSetStr = new String(urlSet);
                        urls = urls + "|" + urlSetStr;
                    }
                    kvsClient.put("pt-index", word, "acc", urls);
                    return Collections.emptyList();
                });
            } catch (Exception e) {
                e.printStackTrace();
            }

            invertedIndex.destroy();
            // Iterator<Row> wordRowInIndex = null;
            // wordRowInIndex = flameContext.getKVS().scan("pt-index");

            // KVSClient kvs = new KVSClient(coordinatorAddr);
            // while (wordRowInIndex.hasNext()) {
            //     Row row = wordRowInIndex.next();
            //     executorService.submit(() -> calculateTfIdf(coordinatorAddr, row, kvs, totalDocuments));
            // }

            flameContext.output("\nEnding indexing job @ " + System.currentTimeMillis());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void calculateTfIdf(String coordinatorAddr, Row row, KVSClient kvs, int totalDocuments) {
        String word = row.key();
        String urls = row.get("acc");
        String[] urlsArr = urls.split("\\|");

        System.out.println("Thread " + Thread.currentThread().threadId() + " " + word + " : Saving TFIDF to kvs");

        List<String> urlArray = Arrays.stream(urlsArr).map(url -> {
            int lastColonIndex = url.lastIndexOf(" _@_@ ");
            int secondLastColonIndex = url.lastIndexOf(" _@_@ ", lastColonIndex - 1);
            int thirdLastColonIndex = url.lastIndexOf(" _@_@ ", secondLastColonIndex - 1);
            return url.substring(0, thirdLastColonIndex);
        }).distinct().toList();

        double inverseDocFreq = Math.log(totalDocuments / urlArray.size());

        Row tfIdfRow = new Row(word);
        for (String u : urlsArr) {
            try {
                int lastColonIndex = u.lastIndexOf(" _@_@ ");
                int secondLastColonIndex = u.lastIndexOf(" _@_@ ", lastColonIndex - 1);
                int thirdLastColonIndex = u.lastIndexOf(" _@_@ ", secondLastColonIndex - 1);

                String url = u.substring(0, thirdLastColonIndex);
                String positions = u.substring(thirdLastColonIndex + 6, secondLastColonIndex);
                Double l2Norm = Double.parseDouble(u.substring(secondLastColonIndex + 6, lastColonIndex));
                String unhashedUrl = u.substring(lastColonIndex + 6);
                double termFreq = (double) positions.split(" ").length / l2Norm;

                double tfidf = termFreq * inverseDocFreq;
                System.out.println("url: " + url + " positions: " + positions + " l2Norm: " + l2Norm + " unhashedUrl: " + unhashedUrl + " tfidf: " + tfidf);

                tfIdfRow.put(url + "_tfidf", String.valueOf(tfidf));
            } catch (Exception e) {
                e.printStackTrace();
                continue;
            }
        }
        try {
            kvs.putRow("pt-tfidf", tfIdfRow);
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("putRow failed for " + word);
        }
    }

    public static String removeStopWords(String input) {
        StringBuilder result = new StringBuilder();
        // Updated regex for tokenization
        String[] words = input.split("\\s+|(?<=\\d)(?=\\D)|(?<=\\D)(?=\\d)");

        for (String word : words) {
            if (!stopwords.contains(word.toLowerCase())) {
                result.append(word).append(" ");
            }
        }

        return result.toString().trim();
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