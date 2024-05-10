package cis5550.ranker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;

import cis5550.jobs.PorterStemmer;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Hasher;

public class Ranker {
    private final String kvsCoordinator;
    private final String searchQuery;
    private final long totalDocuments;
    private ConcurrentHashMap<String, Double> pageRanks;

    public Ranker(String kvsCoordinator, String searchQuery, long totalDocuments, ConcurrentHashMap<String, Double> pageRanks) {
        this.kvsCoordinator = kvsCoordinator;
        this.searchQuery = searchQuery;
        this.totalDocuments = totalDocuments;
        this.pageRanks = pageRanks;
    }

    public static class WordInfo {
        private String word;
        private int frequency;

        public WordInfo(String word, int frequency) {
            this.word = word;
            this.frequency = frequency;
        }

        public String getWord() {
            return word.toLowerCase();
        }

        public int getFrequency() {
            return frequency;
        }
    }

    private List<WordInfo> fetchSearchQuery(String stemmedQuery) {
        if (stemmedQuery == null || stemmedQuery.isBlank()) {
            return Collections.emptyList();
        }
        return Arrays.stream(stemmedQuery.split("\\s+"))
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()))
                .entrySet().stream()
                .map(e -> new WordInfo(e.getKey(), e.getValue().intValue()))
                .collect(Collectors.toList());
    }

    public List<UrlInfo> fetchUrlsForSearchQueryWords(List<WordInfo> wordList) {
        ConcurrentHashMap<String, UrlInfo> urlsInfo = new ConcurrentHashMap<>();
        KVSClient kvsClient = new KVSClient(kvsCoordinator);
        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
        List<Future<?>> futures = new ArrayList<>();
        for (WordInfo words : wordList) {
            futures.add(executorService.submit(() -> {
                try {
                    String word = words.getWord();
                    System.out.println("Thread: " + Thread.currentThread().threadId() + " " + word);
                    if(word.isBlank())
                    {
                        return;
                    }
                    byte[] urlsOfWord = kvsClient.get("pt-index", word, "acc");
                    String urlsOfWordStr = null;
                    List<String> urlArray = new ArrayList<>();
                    if (urlsOfWord != null) {
                        urlsOfWordStr = new String(urlsOfWord);
                        urlArray = Arrays.stream(urlsOfWordStr.split("\\|")).map(url -> {
                            int lastColonIndex = url.lastIndexOf(" _@_@ ");
                            int secondLastColonIndex = url.lastIndexOf(" _@_@ ", lastColonIndex - 1);
                            int thirdLastColonIndex = url.lastIndexOf(" _@_@ ", secondLastColonIndex - 1);
                            String positions = url.substring(thirdLastColonIndex + 6, secondLastColonIndex);
                            String unhashedUrl = url.substring(lastColonIndex + 6).trim();
                            Double l2Norm = Double.parseDouble(url.substring(secondLastColonIndex + 6, lastColonIndex));
                            return unhashedUrl+"|"+(positions.trim().split(" ").length/l2Norm);
                        }).distinct().toList();
                    }
                    double IDF = Math.log(((double) totalDocuments / urlArray.size()));
                    for (String itr : urlArray) {
                        String[] temp = itr.split("\\|");
                        String url = temp[0];
                        double normalizedTermFreq = Double.parseDouble(temp[1]);
                        double tfIdf = normalizedTermFreq * IDF;
                        double pagerank = 0.15;
                        String hashedURL = Hasher.hash(url);
                        if(pageRanks.containsKey(hashedURL)){
                            pagerank = pageRanks.get(hashedURL);
                        }
                        // System.out.println("Scores: "+tfIdf+" "+pagerank);
                        UrlInfo urlInfo = new UrlInfo(url, tfIdf, pagerank);
                        if(urlsInfo.containsKey(url)) {
                            UrlInfo previousInfo = urlsInfo.get(url);
                            urlInfo.addTfidf(previousInfo.getTfIdf());
                        }
                        urlInfo.calculateScore();
                        urlsInfo.put(url, urlInfo);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }));
            
        }
        waitForAll(futures);
        executorService.shutdown();
        List<UrlInfo> list = new ArrayList<>(urlsInfo.values());
        Collections.sort(list, new UrlInfo());
        // for(int i=0;i<list.size();i++) {
        //     System.out.println("Final List("+i+"):"+list.get(i).getUrl()+" "+list.get(i).getScore());
        // }
        return new ArrayList<>(list);
    }

    private void waitForAll(List<Future<?>> futures) {
        for(Future<?> future: futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
            }
        }
    }

    public List<UrlInfo> getUrlInfo()
    {
        PorterStemmer stemmer = new PorterStemmer();
        String query = "";
        String[] tokens = this.searchQuery.split("\\s+");
        for(String token : tokens) {
            stemmer.add(token.toCharArray(), token.length());
            stemmer.stem();
            String stemmedtoken = stemmer.toString();
            if (stemmedtoken.equalsIgnoreCase(token)) {
                query = query + " " + token; // word + stemmed word
            } else {
                query = query + " " + stemmedtoken + " " + token; // word + stemmed word
            }
        }

        List<WordInfo> fetchedWordsInfo = this.fetchSearchQuery(query);
        List<UrlInfo> fetchedUrlInfo = this.fetchUrlsForSearchQueryWords(fetchedWordsInfo);
        return fetchedUrlInfo;
    }
}