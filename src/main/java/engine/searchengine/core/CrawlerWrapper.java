package engine.searchengine.core;

//import com.google.gson.Gson;
//import com.google.gson.reflect.TypeToken;

import com.mongodb.client.*;
import org.bson.Document;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;


import static java.lang.Thread.sleep;

public class CrawlerWrapper {
    private static final int NUM_THREADS = 16;
    ConcurrentSkipListSet<String> VisitedLinks = new ConcurrentSkipListSet<>();
    HashMap<String,String> UniqueUrls = new HashMap<>();
    ConcurrentHashMap<String, Set<String>> NetworkGraph = new ConcurrentHashMap<>();

    ConcurrentSkipListSet<String> CheckedLinks = new ConcurrentSkipListSet<>();

    ConcurrentSkipListSet<String> DisallowedLinks = new ConcurrentSkipListSet<>();

    LinkedList<String> Links = new LinkedList<>();

    public CrawlerWrapper(){
        String[] seedUrls = {
                "https://www.bbc.com/",
                "https://www.espn.com/",
                "https://www.imdb.com/",
                "https://edition.cnn.com/",
                "https://stackoverflow.com/",
                "https://www.amazon.com/",
                "https://www.geeksforgeeks.org/",
                "https://www.forbes.com/",
                "https://www.nature.com/",
                "https://www.skysports.com/",
                "https://www.foxnews.com/",
                "https://www.sciencedaily.com/",
                "https://www.washingtonpost.com/",
                "https://www.nytimes.com/",
                "https://www.theguardian.com/",
                "https://www.ebay.com/"
        };

        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
        MongoDatabase database = mongoClient.getDatabase("SearchEngine");
        MongoCollection<Document> collection = database.getCollection("VisitedLinks");
//        long count = collection.countDocuments();
//        if(count >= 100) {
//            collection.drop();
//            collection = database.getCollection("CrawlerQueue");
//            collection.drop();
//            collection = database.getCollection("NetworkGraph");
//            collection.drop();
//        }

        this.LoadState();
        Thread[] threads = new Thread[NUM_THREADS];
        for (int i = 0; i < seedUrls.length; i++) {
            threads[i] = new Crawler(seedUrls[i], i, VisitedLinks, UniqueUrls, NetworkGraph, Links, CheckedLinks, DisallowedLinks, mongoClient, database).getThread();
        }

        for (int i = 0; i < seedUrls.length; i++) {
            try {
                threads[i].join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        MongoCursor<Document> cursor;

        // Retrieve the network graph documents from its collection as a cursor and iterate over them
        collection = database.getCollection("NetworkGraph");
//        if(collection.countDocuments() == 0) return false;
        cursor = collection.find().iterator();
        // Loop over the collection to insert each document into the HashMap
        while (cursor.hasNext()) {
            // Get the document and extract the key-value pairs
            Document document = cursor.next();
            String name = document.getString("url");
            String referrer = document.getString("referrer");

            // Add the key-value pairs to the HashMap
            if(!NetworkGraph.containsKey(name) && VisitedLinks.contains(name))
                NetworkGraph.put(name, new HashSet<>());
            if(NetworkGraph.containsKey(name) && VisitedLinks.contains(name)) NetworkGraph.get(name).add(referrer);
        }

//        new Crawler("https://www.flickr.com/",0,VisitedLinks,UniqueUrls,NetworkGraph,Links,CheckedLinks,DisallowedLinks, mongoClient, database);
//        new Crawler("https://www.theatlantic.com/video/embed/",1,VisitedLinks,UniqueUrls,NetworkGraph,Links,CheckedLinks,DisallowedLinks, mongoClient, database);
//        new Crawler("https://www.theatlantic.com/video/embed/",2,VisitedLinks,UniqueUrls,NetworkGraph,Links,CheckedLinks,DisallowedLinks, mongoClient, database);

        try {
            sleep(20000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

//        for (Map.Entry<String,Set<String>> entry : NetworkGraph.entrySet()) {
//            if(VisitedLinks.contains(entry.getKey())) System.out.println(entry.getKey() + " " + entry.getValue());
//        }
    }

    public boolean LoadState(){
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
        MongoDatabase database = mongoClient.getDatabase("SearchEngine");
        MongoCollection<Document> collection;
        MongoCursor<Document> cursor;

        // Retrieve the network graph documents from its collection as a cursor and iterate over them
        collection = database.getCollection("VisitedLinks");
        if(collection.countDocuments() == 0) return false;
        cursor = collection.find().iterator();
        // Loop over the collection to insert each document into the HashMap
        while (cursor.hasNext()) {
            // Get the document and extract the key-value pairs
            Document document = cursor.next();
            String url = document.getString("url");
            VisitedLinks.add(url);
        }

        // Retrieve the network graph documents from its collection as a cursor and iterate over them
        collection = database.getCollection("UniqueUrls");
        if(collection.countDocuments() == 0) return false;
        cursor = collection.find().iterator();
        // Loop over the collection to insert each document into the HashMap
        while (cursor.hasNext()) {
            // Get the document and extract the key-value pairs
            Document document = cursor.next();
            String pageHash = document.getString("pageHash");
            String url = document.getString("url");
            UniqueUrls.put(pageHash,url);
        }

        // Retrieve the network graph documents from its collection as a cursor and iterate over them
        collection = database.getCollection("UniqueUrls");
        if(collection.countDocuments() == 0) return false;
        cursor = collection.find().iterator();
        // Loop over the collection to insert each document into the HashMap
        while (cursor.hasNext()) {
            // Get the document and extract the key-value pairs
            Document document = cursor.next();
            String pageHash = document.getString("pageHash");
            String url = document.getString("url");
            UniqueUrls.put(pageHash,url);
        }

        // Retrieve the network graph documents from its collection as a cursor and iterate over them
        collection = database.getCollection("CrawlerQueue");
        if(collection.countDocuments() == 0) return false;
        cursor = collection.find().sort(new Document("_id", 1)).iterator();
        // Loop over the collection to insert each document into the HashMap
        while (cursor.hasNext()) {
            // Get the document and extract the key-value pairs
            Document document = cursor.next();
            String url = document.getString("url");
            Links.add(url);
        }

        // Retrieve the network graph documents from its collection as a cursor and iterate over them
        collection = database.getCollection("NetworkGraph");
        if(collection.countDocuments() == 0) return false;
        cursor = collection.find().iterator();
        // Loop over the collection to insert each document into the HashMap
        while (cursor.hasNext()) {
            // Get the document and extract the key-value pairs
            Document document = cursor.next();
            String name = document.getString("url");
            String referrer = document.getString("referrer");

            // Add the key-value pairs to the HashMap
            if(!NetworkGraph.containsKey(name) && VisitedLinks.contains(name))
                NetworkGraph.put(name, new HashSet<>());
            if(NetworkGraph.containsKey(name) && VisitedLinks.contains(name)) NetworkGraph.get(name).add(referrer);
        }

        return true;
    }

}
