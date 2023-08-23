package engine.searchengine.core;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class PageRank {
    double DAMPING_FACTOR=0.85;
    Map<String,Double> docPopularityScore=new HashMap<String,Double>();
    Map<String,Set<String>> Network=new HashMap<String,Set<String>>();//String refers to the url whilst the Set refers to all the links pointing at this url
    Map<String,Integer> numLinksPerURL=new HashMap<>();
    public PageRank(Map<String,Set<String>>network)
    {
        Network=network;
        double initScore=1.0;
        System.out.println("initScore: "+initScore);
        for(Map.Entry<String,Set<String>> node:network.entrySet()) {
            docPopularityScore.put(node.getKey(), initScore);
            System.out.println("Node: " + node.getKey());
            numLinksPerURL.put(node.getKey(),0);
        }
        for(Map.Entry<String,Set<String>> node:network.entrySet()) {
            for(String linkingURL:node.getValue())
            {
                if(numLinksPerURL.get(linkingURL)!=null)
                    numLinksPerURL.put(linkingURL,numLinksPerURL.get(linkingURL)+1);
            }
        }
    }

    private void updateScores()
    {
        for(Map.Entry<String,Set<String>> node:Network.entrySet())
        {
            double totalScore=(1-DAMPING_FACTOR)/Network.size();
            for(String linkingURL:node.getValue())
            {
                if(docPopularityScore.get(linkingURL) != null && numLinksPerURL.get(linkingURL) != null)
                    totalScore+=DAMPING_FACTOR*docPopularityScore.get(linkingURL)/numLinksPerURL.get(linkingURL);
            }
            docPopularityScore.put(node.getKey(), totalScore);

        }
    }
    public void updateDB()
    {
        MongoDatabase database;
        MongoClient mongoClient;
        mongoClient = MongoClients.create("mongodb://localhost:27017");
        database = mongoClient.getDatabase("SearchEngine");
        try {

            database.createCollection("PageRank");
        }
        catch (Exception e)
        {}
        MongoCollection collection=database.getCollection("PageRank");
        //Document document = new Document();
        for (Map.Entry<String,Double> node:docPopularityScore.entrySet()) {

            Document document = new Document();
            //if(collection.find(eq("url",node.getKey()))!=null)
            if(collection.countDocuments(Filters.eq("url",node.getKey())) > 0)
              collection.updateOne(Filters.eq("url",node.getKey()),Updates.set("score",node.getValue()));
            else {
                document.append("url", node.getKey());
                document.append("score", node.getValue());
                collection.insertOne(document);
            }
        }

    }
    public void run()
    {
        for(int i=0;i<docPopularityScore.size()+1;i++)
            this.updateScores();
        this.updateDB();
    }
    public void print()
    {
        System.out.println(docPopularityScore.toString());
        System.out.println("--------------------------------------");
        System.out.println(numLinksPerURL.toString());
    }

}
