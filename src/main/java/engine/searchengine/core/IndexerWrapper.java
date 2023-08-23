package engine.searchengine.core;

import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;
import com.mongodb.client.*;
import org.bson.Document;

import javax.print.Doc;
import java.io.File;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class IndexerWrapper {
    private static List<String> visitedLinksList;
    private HashMap<String, String> tokensID = new HashMap<>();
    public IndexerWrapper(){
//        MongoClientOptions options = MongoClientOptions.builder()
//                .connectionsPerHost(6000)
//                .build();
//        MongoClientURI mongoUri = new MongoClientURI("mongodb://localhost:27017", options);
        ServerAddress serverAddress = new ServerAddress("localhost", 27017);
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyToConnectionPoolSettings(builder ->
                        builder.maxSize(100)
                                .maxWaitQueueSize(6000))
                .applyToClusterSettings(builder ->
                        builder.hosts(Arrays.asList(serverAddress)))
                .build();
        MongoClient mongoClient = MongoClients.create(settings);
        MongoDatabase database = mongoClient.getDatabase("SearchEngine");
        MongoCollection<Document> collection = database.getCollection("VisitedLinks");
        MongoCollection<Document> DocsCollection = database.getCollection("Documents");
        MongoCollection<Document> InvCollection = database.getCollection("InvertedFile");
        MongoCollection<Document> ImgCollection = database.getCollection("Images");
        if(collection.countDocuments() == 0) {
            System.out.println("No Documents Found in MongoDB");
        }


//        if (visitedLinksDocument == null){
//            System.out.println("No State Found in MongoDB");
//        }
        CountDownLatch latch = new CountDownLatch(6000);
//        ExecutorService executor = Executors.newFixedThreadPool(100);
        Indexer indexer = new Indexer(latch, mongoClient, database, InvCollection, DocsCollection, ImgCollection, tokensID);
//        MongoCursor<Document> cursor;
//        cursor = collection.find().iterator();
//        // Loop over the collection to insert each document into the HashMap
//        while (cursor.hasNext()) {
//            // Get the document and extract the key-value pairs
//            Document document = cursor.next();
//            String url = document.getString("url");
//            String path = document.getString("path");
//            System.out.println("Indexing URL: " + path);
//            executor.submit(
//                    new Thread(() -> {
//                        new Indexer(latch, mongoClient, database, InvCollection, DocsCollection, ImgCollection, tokensID).index(url, path, FetchType.FILE);
//                    })
//            );
////            indexer.index(url,path, FetchType.FILE);
////            new File(path).delete();
////            Thread thread = new Thread(() -> {
////                // Code to be executed by the thread
////                new Indexer(latch, mongoClient, database, InvCollection, DocsCollection, ImgCollection, tokensID).index(url, path, FetchType.FILE);
////            });
////
////            // Set the thread priority to the highest value
////            thread.setPriority(Thread.MAX_PRIORITY);
////
////            // Start the thread
////            thread.start();
//        }
//
//        try {
//            latch.await();
//
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        indexer.UpdateIDF();

//        executor.shutdown();

        File folder = new File("D:\\CUFE\\CMP-02\\Sem2\\APT\\Project\\FetchitRepo\\Fetchit\\src\\main\\resources");
        File[] files = folder.listFiles();

        if (files != null) {
            for (File file : files) {
                if (file.getName().endsWith(".html")) {
                    file.delete();
                    System.out.println("Deleted file: " + file.getName());
                }
            }
        }
    }
}
