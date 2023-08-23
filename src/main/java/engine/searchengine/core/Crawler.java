package engine.searchengine.core;

//import ch.qos.logback.classic.Level;
//import ch.qos.logback.classic.Logger;
//import ch.qos.logback.classic.LoggerContext;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOptions;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

public class Crawler implements Runnable {
    // Maximum depth to crawl
    private static final int MAX_DEPTH = 2;
    private static final int NUM_TO_CRAWL = 6000;
    // Thread used to run this crawler
    private final Thread Th;
    // Seed URL to start crawling
    private final String FirstLink;
    // Set of visited URLs
    private static ConcurrentSkipListSet<String> VisitedLinks;

    // ID of the crawler
    private final int Id;

    // User-Agent string to identify the crawler
    private static final String USER_AGENT = "CrawlerIdentifier"; // To be changed

    // Delay between requests to the same domain
    private static final int CRAWL_DELAY = 1000;

    // Map to store the last time a domain was crawled
    private static final HashMap<String, Long> DomainLastCrawlTime = new HashMap<>();


    private static HashMap<String, String> UniqueUrls;

    private static LinkedList<String> Queue;

    public static int CrawledLinksCount = 0;

    private static ConcurrentHashMap<String,Set<String>> NetworkGraph;

    private final ConcurrentSkipListSet<String> CheckedLinks;

    private final ConcurrentSkipListSet<String> DisallowedLinks;

    private final MongoClient mongoClient;
    private final MongoDatabase database;

    // Constructor for the crawler
    public Crawler(String seed, int num, ConcurrentSkipListSet<String> visitedLinks, HashMap<String, String> uniqueUrls, ConcurrentHashMap<String,Set<String>> networkGraph, LinkedList<String> queue,ConcurrentSkipListSet<String> checkedLinks,ConcurrentSkipListSet<String> disallowedLinks, MongoClient mongoClient, MongoDatabase database) {
        this.mongoClient = mongoClient;
        this.database = database;
        FirstLink = seed;
        Id = num;
        VisitedLinks = visitedLinks;
        UniqueUrls = uniqueUrls;
        NetworkGraph = networkGraph;
        Queue = queue;
        CheckedLinks = checkedLinks;
        DisallowedLinks = disallowedLinks;
        // Start a new thread to run this crawler
        Th = new Thread(this);
        Th.start();

    }

    // Runnable method that is called when the thread starts
    @Override
    public void run() {
        CrawlBFS(FirstLink);
    }

    // Crawls the web starting from the given URL using breadth first search
    public void CrawlBFS(String url) {

        Fingerprint FingerPrinter = new Fingerprint();

        String pageHash = null;

        // Add the initial URL to the queue
        synchronized (Queue){
            Queue.add(url);
        }

        // Process the URLs in the queue until it is empty
        while (!Queue.isEmpty() && VisitedLinks.size() < NUM_TO_CRAWL) {
            // Get the next URL from the queue
            String currentUrl;

            synchronized (Queue){
                currentUrl = Queue.poll();
            }
            try {
                // Check if the domain can be crawled based on the crawl delay
                if (getVisitedLinks().contains(currentUrl) || !CanCrawlDomain(currentUrl)) continue;
                // Send a HTTP request to the URL and get the response
                Connection connection = Jsoup.connect(currentUrl)
                        .userAgent(USER_AGENT)
                        .referrer("http://www.google.com")
                        .ignoreContentType(true)
                        .timeout(5000);
                Document doc = connection.get();
                File file;
                Path path;
                synchronized (VisitedLinks){
                    file = new File("../websites/document"+ UUID.randomUUID() +".html");
                }
                path = Paths.get(file.getAbsolutePath());
                Files.write(path, doc.outerHtml().getBytes());

                String host = new URL(currentUrl).getHost();
                String icon = "";
                if(!doc.select("link[rel~=(?i)icon][rel~=(?i)shortcut]").attr("href").equals(""))
                {
                    icon = doc.select("link[rel~=(?i)icon][rel~=(?i)shortcut]").attr("href") ;
                    if(!icon.contains("http"))
                        icon = "https://" + host + icon;
                }
                else if(!doc.select("link[rel=apple-touch-icon]").attr("href").equals("")){
                    icon = doc.select("link[rel=apple-touch-icon]").attr("href");
                    if(!icon.contains("http"))
                        icon = "https://" + host + icon;
                }

                if(icon.contains(".ico")) icon = "";

                String title = "";

                if(!doc.title().equals(""))
                    title = doc.title();
                else continue;

                // Process the links on the page
//                System.out.println("Crawler " + this.Id + " Started NG : " + currentUrl + " at " + LocalDateTime.now());
                for (Element link : doc.select("a[href]")) {
                    // Get the absolute URL of the linkCanCrawlDomain
                    String newUrl = link.absUrl("href");

                    synchronized (Queue){
                        Queue.add(newUrl);
                        SaveQueueEntry(newUrl);
                    }

                    synchronized (NetworkGraph){
                        if(!NetworkGraph.containsKey(newUrl))
                            NetworkGraph.put(newUrl, new HashSet<>());
                        else
                            if(NetworkGraph.get(newUrl).contains(currentUrl)) continue;

                        NetworkGraph.get(newUrl).add(currentUrl);
                    }
                    SaveNetworkGraphEntry(newUrl, currentUrl);

                }

                try {
                    pageHash = Fingerprint.getPageHash(currentUrl);
                } catch (IOException e) {
                    System.out.println("Error in calculating hash");
                    throw new RuntimeException(e);
                } catch (NoSuchAlgorithmException e) {
                    System.out.println("Error in calculating hash algorithm");
                    throw new RuntimeException(e);
                }

                // Add the URL to the set of visited URLs


                addVisitedLink(currentUrl,path,pageHash,doc.title(),icon);

            } catch (IOException e) {
                // Continue if there is an exception while crawling the page
            }
        }
    }

    public synchronized void addVisitedLink(String url,Path path,String pageHash,String title, String icon){
        if(VisitedLinks.size() < NUM_TO_CRAWL && !UniqueUrls.containsKey(pageHash)) {
            UniqueUrls.put(pageHash, url);
            SaveUniqueUrlsEntry(pageHash, url);
            if(!VisitedLinks.contains(url)){
                synchronized (VisitedLinks){
                    VisitedLinks.add(url);
                    SaveVisitedLinksEntry(url,path,title,icon);
                    System.out.println("Crawler " + Id + " found link number " + VisitedLinks.size()  + " : " + url);
                }
            }
        }
    }

    public static ConcurrentHashMap<String, Set<String>> getNetworkGraph() {
        return NetworkGraph;
    }

    // Recursive method to crawl web pages
    private void Crawl(String url, int depth) {
        // Check if maximum depth has been reached
        if (depth < MAX_DEPTH) {
            try {
                // Check if the domain can be crawled based on the crawl delay
                if (!CanCrawlDomain(url)) {
                    return;
                }

                // Send a HTTP request to the URL and get the response
                Connection connection = Jsoup.connect(url)
                        .userAgent(USER_AGENT)
                        .referrer("http://www.google.com")
                        .ignoreContentType(true)
                        .timeout(30000);
                Document Doc = connection.get();

                // Iterate through all the links on the page
                for (Element link : Doc.select("a[href]")) {
                    // Get the absolute URL of the link
                    String NewLink = link.absUrl("href");
                    System.out.println("Crawler " + Id + " found link: " + NewLink);
                    // Check if the link has not been visited before
                    if (!VisitedLinks.contains(NewLink)) {
                        // Recursively crawl the new link
                        Crawl(NewLink, depth + 1);
                    }
                }

                // Add the URL to the set of visited URLs
                VisitedLinks.add(url);
            } catch (IOException e) {
                // Continue if there is an exception while crawling the page
            }
        }
    }

    // Method to check if a domain can be crawled based on the crawl delay and robots.txt rules
    private boolean CanCrawlDomain(String url) {
        try {
            // Get the domain from the URL
            String domain = new URL(url).getHost();
            if (CheckedLinks.contains(domain)) {
                return DisallowedLinks.contains(url);
            }
            CheckedLinks.add(domain);
            // Check if the domain has been crawled before
            if (DomainLastCrawlTime.containsKey(domain)) {
                // Get the last crawl time for the domain
                long lastCrawlTime = DomainLastCrawlTime.get(domain);

                // Calculate the time elapsed since the last crawl
                long elapsedTime = System.currentTimeMillis() - lastCrawlTime;

                // Check if the elapsed time is less than the crawl delay
                if (elapsedTime < CRAWL_DELAY) {
                    // Wait for the remaining time before crawling again
                    Thread.sleep(CRAWL_DELAY - elapsedTime);
                }
            }

            // Update the last crawl time for the domain
            DomainLastCrawlTime.put(domain, System.currentTimeMillis());

            // Check if the domain allows crawling based on the robots.txt rules
            String robotsUrl = "http://" + domain + "/robots.txt";
            Connection connection = Jsoup.connect(robotsUrl);
            Document robots = connection.get();
            Connection.Response response = Jsoup.connect(robotsUrl).execute();
            String text = response.body();
            // Check if the robots.txt file exists and can be accessed
            if (connection.response().statusCode() == 200) {
                // Parse the robots.txt file and check if the agent is allowed to crawl
                String userAgent = USER_AGENT.toLowerCase();
                String[] lines = text.split("\\r?\\n");
                for (int i = 0; i < lines.length; i++) {
                    String line = lines[i].trim();
                    if (line.toLowerCase().startsWith("user-agent:")) {
                        if (line.toLowerCase().contains(userAgent) || line.toLowerCase().contains(": *")) {
                            // The agent is specified in the robots.txt file
                            // Check if it is allowed to crawl
                            while (i < lines.length - 1) {
                                line = lines[++i].trim();
                                if (line.toLowerCase().startsWith("user-agent:")) {
                                    i--;
                                    break;
                                } else if (line.toLowerCase().startsWith("disallow:")) {
                                    String disallow = line.substring("Disallow:".length()).trim();
                                    if (url.toLowerCase().endsWith(disallow.toLowerCase())) {
                                        // The URL is disallowed by the robots.txt file
                                        DisallowedLinks.add(url);
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // The domain can be crawled
            return !DisallowedLinks.contains(url);
        } catch (Exception e) {
            // Return false if there is an exception while checking the domain
            return false;
        }
    }

    public static ConcurrentSkipListSet<String> getVisitedLinks() {
        return VisitedLinks;
    }

    public void PrintVisitedLinks() {
        for (String link : VisitedLinks) {
            System.out.println(link);
        }
    }

    public Thread GetThread() {
        return Th;
    }

    public void SaveQueueEntry(String url) {
        MongoCollection<org.bson.Document> collection = database.getCollection("CrawlerQueue");
        org.bson.Document doc = new org.bson.Document("url", url);
        collection.insertOne(doc);
    }
    public void SaveUniqueUrlsEntry(String hash, String url) {
//        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
//        MongoDatabase database = mongoClient.getDatabase("SearchEngine");
        MongoCollection<org.bson.Document> collection = database.getCollection("UniqueUrls");
        org.bson.Document doc = new org.bson.Document("pageHash", hash).append("url", url);
        collection.insertOne(doc);
//        mongoClient.close();
    }

    public void SaveVisitedLinksEntry(String url, Path path, String title, String icon) {
        MongoCollection<org.bson.Document> collection = database.getCollection("VisitedLinks");
        org.bson.Document doc = new org.bson.Document("url", url).append("path",path.toString()).append("title", title).append("icon", icon);
        collection.insertOne(doc);
    }

    public void SaveNetworkGraphEntry(String url, String referrer){
//        MongoCollection<org.bson.Document> collection = database.getCollection("NetworkGraph");
//        org.bson.Document filter = new org.bson.Document("url", key);
//        org.bson.Document update = new org.bson.Document("$set", new org.bson.Document("referers", value));
//        UpdateOptions options = new UpdateOptions().upsert(true);
//        collection.updateOne(filter, update, options);
        MongoCollection<org.bson.Document> collection = database.getCollection("NetworkGraph");
        org.bson.Document doc = new org.bson.Document("url", url).append("referrer",referrer);
        collection.insertOne(doc);
    }

    public Thread getThread() {
        return Th;
    }
}