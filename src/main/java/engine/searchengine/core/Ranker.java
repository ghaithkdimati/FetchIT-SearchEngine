package engine.searchengine.core;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.tartarus.snowball.ext.EnglishStemmer;

import java.util.*;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.eq;


public class Ranker {
    String originalSearchQuery;
    String searchQuery;
    String[] searchQueryArr;
    String[] originalSearchQueryArr;

    static HashMap<String,String> occurrencesMap = new HashMap<String,String>();

    //read from inverted file documents that containing this words
    // calculating TF-IDF
//    String[] searchQueryArr=searchQuery.split(" ");
//    String[] originalSearchQueryArr=searchQuery.split(" ");
    Map<String,Double> docRelevanceScore=new HashMap<String,Double>();
    MongoDatabase database  ;
    MongoClient mongoClient;
    private MongoCollection<Document> collection, DocsCollection,pageRankCollection;
    Map<String,Double> tagsWeights=new HashMap<String,Double>();

    public void setSearchQuery(String searchQuery)
    {

        this.originalSearchQuery=searchQuery.toLowerCase(Locale.ROOT);
        this.originalSearchQueryArr=searchQuery.split(" ");
        searchQuery=searchQuery.toLowerCase(Locale.ROOT);
        this.searchQueryArr=searchQuery.split(" ");
        //System.out.println(searchQueryArr);
        EnglishStemmer stemmer = new EnglishStemmer();
        for(int i=0;i<searchQueryArr.length;i++)
        {
            stemmer.setCurrent(searchQueryArr[i]);
            stemmer.stem();
            searchQueryArr[i]=stemmer.getCurrent();
        }

        System.out.println(searchQueryArr);


    }


    public Ranker()
    {
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
        database = mongoClient.getDatabase("SearchEngine");
        collection = database.getCollection("InvertedFile");
        pageRankCollection=database.getCollection("PageRank");
        //setting weights of html tags
        //source:https://www.researchgate.net/figure/A-list-of-example-HTML-tags-and-corre-sponding-significance-weights_tbl1_254463711
        tagsWeights.put("h1",1.0);
        tagsWeights.put("h2",0.85);
        tagsWeights.put("h3",0.7);
        tagsWeights.put("h4",0.5);
        tagsWeights.put("h5",0.3);
        tagsWeights.put("h6",0.1);
        tagsWeights.put("title",1.0);
        tagsWeights.put("p",0.1);
        tagsWeights.put("i",0.3);
        tagsWeights.put("b",0.8);
        tagsWeights.put("big",0.8);
        tagsWeights.put("li",0.2);
        tagsWeights.put("u",0.7);
        tagsWeights.put("th",0.5);
        tagsWeights.put("strong",0.8);
        tagsWeights.put("em",0.5);

    }

    public void calculateRelevancy()
    {
        //MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
        //MongoDatabase database = mongoClient.getDatabase("SearchEngine");

        Bson projectionFields = Projections.fields(
                Projections.include("token","DF","Docs","IDF"),
                Projections.excludeId());


        List<Document> pages;
        // System.out.println(pages);//accessed the page
        //calculate relevancy
        //iterating on words
        Document doc;
        int k=0;
        for (String token:searchQueryArr)
        {
            HashMap<String,Boolean> wordExists = new HashMap<String,Boolean>();
//            pages=(List<Document>)doc.get("Docs");
            Map<String,Boolean> visitedPage;
            doc=collection.find(eq("token",token)).projection(projectionFields).first();
            if(doc==null)
                continue;
            pages=(List<Document>)doc.get("Docs");
            double DF=Double.parseDouble(doc.get("DF").toString());
            // System.out.println(DF);
            double TF;
            double IDF=Double.parseDouble(doc.get("IDF").toString());
            double totalTagWeight=0;
            //iterating on Docs that contain token
            System.out.println(IDF);
            System.out.println(token);
            System.out.println(pages);
            for (int i = 0; i < pages.size(); i++)
            {
                //getting tag weight default value is of <p>
                String tag=pages.get(i).get("tag").toString();
                double tagWeight=(tagsWeights.containsKey(tag))?tagsWeights.get(tag):tagsWeights.get("p");
                //getting the page getting processed
                TF=Double.parseDouble(pages.get(i).get("TF").toString());
                String originalWord=pages.get(i).get("originalWord").toString();
                String currentPage=pages.get(i).get("page").toString();
                String occurrence=pages.get(i).get("occurrence").toString();

                //double pageRank=Double.parseDouble(pageRankCollection.find(eq("url",currentPage)).projection(pageRankProjectionFiels).first().get("score").toString());
                //System.out.println(pageRank);
                System.out.println(currentPage+"\t"+originalWord);
                double rightWordWeight=(originalWord.equals(originalSearchQueryArr[k]))?1:0.8;
                if(docRelevanceScore.containsKey(currentPage)){
                    if(!wordExists.containsKey(currentPage))
                        docRelevanceScore.put(currentPage,docRelevanceScore.get(currentPage)+TF*IDF*tagWeight*rightWordWeight+1);
                    else
                        docRelevanceScore.put(currentPage,docRelevanceScore.get(currentPage)+TF*IDF*tagWeight*rightWordWeight);
                }
                else
                    docRelevanceScore.put(currentPage,TF*IDF*tagWeight*rightWordWeight);
                if(!occurrencesMap.containsKey(token+","+currentPage))
                    occurrencesMap.put(token+","+currentPage,occurrence);
                wordExists.put(currentPage,true);
            }
            k++;

        }
        System.out.println("--------------------------------------");

        docRelevanceScore.forEach((key, value) -> System.out.println(key + " " + value));
        //System.out.println("-----------------------");
        //System.out.println(docRelevanceScore);

    }

    public void calculatePopularity()
    {

        Bson pageRankProjectionFields=Projections.fields(Projections.include("url","score"),Projections.excludeId());

        for(Map.Entry<String,Double> page:docRelevanceScore.entrySet())
        {
            System.out.println("/////////////////////////");
            double pageRank=0.0;
            Document pageRankDoc=pageRankCollection.find(eq("url",page.getKey())).first();
            if(pageRankDoc!=null)
                pageRank=Double.parseDouble(pageRankDoc.get("score").toString());
            System.out.println(page.getKey()+"          "+pageRank);
            docRelevanceScore.put(page.getKey(),0.8*page.getValue()+0.2*pageRank);
        }
    }

    public Map<String,Double> getDocRelevanceScore()
    {
        docRelevanceScore=docRelevanceScore.entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));;
        return docRelevanceScore;
    }

    public void run()
    {
        this.calculateRelevancy();
        this.calculatePopularity();
    }

    public static HashMap<String,String> getOccurrencesMap()
    {
        return occurrencesMap;
    }

}
