package cis5550.ranker;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import cis5550.tools.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import cis5550.webserver.Server;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import java.io.BufferedReader;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class RankerStarter {
    private static final Logger logger = Logger.getLogger(RankerStarter.class);

    public static void main(String[] args) throws IOException {
        if(args.length != 2) {
            throw new IllegalArgumentException("Incomplete Input arguments");
        }

        String kvsAddr = args[1];
        KVSClient client = new KVSClient(kvsAddr);
        // final long totalDocuments = client.count("pt-crawl");
        final long totalDocuments = 202609;
        System.out.println("Total Documents: " + totalDocuments);
        List<String> indexList = new ArrayList<>();
        Iterator<Row> iterator = client.scan("pt-index");
        while (iterator.hasNext()) {
            indexList.add(iterator.next().key());
        }
        ConcurrentHashMap<String, Double> pageRanks = new ConcurrentHashMap<>();
        
        Iterator<Row> itr = client.scan("pt-pageranks");
        while(itr.hasNext()) {
            Row r = itr.next();
            String rank = r.get("rank");
            if(rank!=null) {
                try{
                    Double rankValue = Double.parseDouble(rank);
                    pageRanks.put(r.key(), rankValue);
                }
                catch(Exception e) {
                    continue;
                }
            }
        }
        
        System.out.println("loaded : " + indexList.size() + " words");

        Server.port(Integer.parseInt(args[0]));
        Server.staticFiles.location("frontend");
        Server.get("/", ((request, response) -> {
            String filePath = "./frontend/EntryPage.html";
            System.out.println("Home page route");
            StringBuilder htmlBuilder = new StringBuilder();

            try (BufferedReader reader = Files.newBufferedReader(Paths.get(filePath))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    htmlBuilder.append(line).append("\n");
                }
            } catch (IOException e) {
                logger.error("Error reading file " + filePath, e);
            }
            response.type("text/html");
            return htmlBuilder.toString();
        }));

        Server.get("/search", (request, response) -> {
            System.out.println("Search route");
            String query = URLDecoder.decode(request.queryParams("query"), StandardCharsets.UTF_8);

            JSONArray array = new JSONArray();
            Ranker ranker = new Ranker(args[1], query, totalDocuments, pageRanks);
            List<UrlInfo> result = ranker.getUrlInfo();

            // Directly add each UrlInfo's URL to the JSON array
            for (UrlInfo urlRank : result) {
                JSONObject obj = new JSONObject();
                obj.put("url", urlRank.getUrl());
                array.put(obj);
            }

            response.type("application/json");
            return array.toString();
        });

        Server.get("/indexList", (req, res) -> {
            JSONArray jsonArray = new JSONArray();
            jsonArray.put(indexList);
            return jsonArray.toString();
        });
    }
}