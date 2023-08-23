package engine.searchengine.controllers;

import ch.qos.logback.core.joran.sanity.Pair;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import engine.searchengine.Models.ImageObject;
import engine.searchengine.Models.ResultObject;
import engine.searchengine.core.QueryProcessor;
import org.bson.Document;
import engine.searchengine.core.Ranker;
import org.bson.conversions.Bson;
import org.jsoup.Jsoup;
import org.jsoup.select.Elements;
import org.springframework.boot.web.servlet.error.ErrorController;
import org.springframework.web.bind.annotation.*;

//import javax.swing.text.Document;
import javax.swing.text.Element;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.sql.Time;
import java.time.LocalDateTime;
import java.util.*;


@RestController
@RequestMapping("query")
public class SearchController implements ErrorController {

    private final int PAGE_COUNT = 10;
    private static final MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
    private static final MongoDatabase database = mongoClient.getDatabase("SearchEngine");

    private static final MongoCollection imgCollection = database.getCollection("Images");
    private static List<String> urls;
    private static final List<String> stopwords = Collections.unmodifiableList(Arrays.asList(
            "a", "an", "and", "are", "as", "at", "be", "but", "by", "for",
            "if", "in", "into", "is", "it", "no", "not", "of", "on", "or",
            "such", "that", "the", "their", "then", "there", "these", "they",
            "this", "to", "was", "will", "with", "about", "above", "after", "again",
            "against", "all", "am", "any", "because", "been", "before", "being", "below",
            "between", "both", "can", "could", "did", "do", "does", "doing", "down", "during",
            "each", "few", "from", "further", "had", "has", "have", "having", "he", "her",
            "here", "hers", "herself", "him", "himself", "his", "how", "i", "if", "in", "into",
            "is", "it", "its", "itself", "me", "more", "most", "my", "myself", "no", "nor",
            "not", "now", "on", "once", "only", "or", "other", "our", "ours", "ourselves",
            "out", "over", "own", "same", "she", "should", "so", "some", "such", "than",
            "that", "the", "their", "theirs", "them", "themselves", "then", "there", "these",
            "they", "this", "those", "through", "to", "too", "under", "until", "up", "very",
            "was", "we", "were", "what", "when", "where", "which", "while", "who", "whom",
            "why", "with", "you", "your", "yours", "yourself", "yourselves",
            "able", "against", "almost", "among", "around", "because", "been", "being", "best",
            "better", "between", "big", "both", "by", "came", "can", "cannot", "come", "could",
            "did", "different", "do", "does", "done", "down", "each", "end", "even", "every",
            "few", "find", "first", "for", "found", "from", "get", "give", "good", "great",
            "had", "has", "have", "having", "he", "her", "here", "herself", "him", "himself",
            "his", "how", "however", "i'd", "i'll", "i'm", "i've", "if", "into", "its", "just",
            "know", "large", "last", "like", "little", "look", "made", "make", "many", "may",
            "me", "most", "much", "must", "my", "never", "new", "now", "of", "off", "often"
    ));

    private static String searchQuery;
    private int resultsCount = 0;

    private List<ImageObject> images = new ArrayList<>();


    public static String getTitle(String url)  {
        MongoCollection<Document> collection = database.getCollection("VisitedLinks");
        Document filter = new Document("url", url);
        Document result = collection.find(filter).first();

        return result.getString("title");
    }
    public static String getIcon(String url) throws URISyntaxException {
        MongoCollection<Document> collection = database.getCollection("VisitedLinks");
        Document filter = new Document("url", url);
        Document result = collection.find(filter).first();

        return result.getString("icon");
    }
    public static String getMostMatchingParagraph(String url,String query) {
        String mostMatchingParagraph = "";
        Map<String,Integer> text = new HashMap<>();

        MongoCollection<Document> collection = database.getCollection("InvertedFile");
        List<String> wordList= Arrays.stream(query.toLowerCase().split(" ")).toList();
        HashMap<String,String> map = Ranker.getOccurrencesMap();
        for(String word : wordList){

            if(stopwords.contains(word)) continue;

            System.out.println("StartedMostMatchingParagraph at : " + LocalDateTime.now());

            if(map.containsKey(word + "," + url)){
                mostMatchingParagraph += map.get(word + "," + url);
                mostMatchingParagraph += "...";
            }

            System.out.println("FinishedMostMatchingParagraph at : " + LocalDateTime.now());

            if(mostMatchingParagraph.length() > 200) {
                mostMatchingParagraph += "...";
                break;
            }
        }


        return mostMatchingParagraph;
    }


    @GetMapping("/count")
    public int count() {
        return resultsCount;
    }

    @GetMapping("/images")
    public List<ImageObject> images() {
        return images;
    }

    @GetMapping
    public List<ResultObject> search(@RequestParam String query, @RequestParam int page) {
//        query = "hello \"real madrid\" my man";
        query = query.toLowerCase();
        System.out.println("StartedQuery at :" + LocalDateTime.now());
        Map<String, Double> docRelevanceScore;
        int firstQuoteIndex = -1;
        int lastQuoteIndex = -1;
        QueryProcessor queryProcessor = new QueryProcessor();
        if(searchQuery == null || query != searchQuery){
            // ex : hi "hello world" no // size = 19 fIdx = 3 lIdx = 15
            String phraseSearchQuery = "";
            firstQuoteIndex = query.indexOf('"');
            System.out.println("firstQuoteIndex : " + firstQuoteIndex);
            lastQuoteIndex = query.indexOf('"', firstQuoteIndex + 1);
            System.out.println("lastQuoteIndex : " + lastQuoteIndex);
            if(firstQuoteIndex != -1 && lastQuoteIndex != -1){
//                phraseSearchQuery = query.substring(firstQuoteIndex + 1, lastQuoteIndex);
//                System.out.println(phraseSearchQuery);
//                try {
//                    queryProcessor.PhraseSearch(phraseSearchQuery);
//                } catch (IOException e) {
//                    throw new RuntimeException(e);
//                } catch (NoSuchAlgorithmException e) {
//                    throw new RuntimeException(e);
//                }

                queryProcessor.processQuery(query);

                docRelevanceScore = queryProcessor.getDocRelevanceScore();
                urls = docRelevanceScore.keySet().stream().toList();

            }else{
                System.out.println("Ranker Started at : " + LocalDateTime.now());
                Ranker ranker = new Ranker();

                ranker.setSearchQuery(query);

                ranker.calculateRelevancy();

                ranker.calculatePopularity();

                docRelevanceScore = ranker.getDocRelevanceScore();

                urls = docRelevanceScore.keySet().stream().toList();

                System.out.println("Ranker Finished at : " + LocalDateTime.now());

            }

            images = new ArrayList<>();
            int count = 0;
            for (String url : urls) {
                FindIterable<Document> imgs = imgCollection.find(Filters.eq("page", url)).limit(10);
                for(Document doc : imgs){
                    images.add(new ImageObject(doc.getString("page"), doc.getString("alt") , doc.getString("url")));
                }
                if(count > 10)
                    break;
                count++;
            }
            resultsCount = docRelevanceScore.size();

        }


        List<String> paragraphs = new ArrayList<>();
        String mostMatchingParagraph = "";
        List<ResultObject> results = new ArrayList<>();
        for (int i = (page-1) * PAGE_COUNT; i < (page)* PAGE_COUNT;i++) {
            if(urls.size() > i) {
                if (firstQuoteIndex != -1 && lastQuoteIndex != -1) {
                    Map<String, String> occurences = queryProcessor.getOccurencesMap();
                    System.out.println("occurences is : "+occurences);
                    System.out.println("query is : "+query);
                    if(occurences.containsKey(query+","+urls.get(i))) mostMatchingParagraph = occurences.get(query+","+urls.get(i)); else mostMatchingParagraph = "";
                } else
                    mostMatchingParagraph = getMostMatchingParagraph(urls.get(i), query);

                try {
                    mostMatchingParagraph = mostMatchingParagraph.substring(0, Math.min(mostMatchingParagraph.length(), 200));
                }catch (Exception e) {
                    System.out.println("The Occurence is :");
                    throw new RuntimeException(e);
                }

                URL url = null;

                try {
                    url = new URL(urls.get(i));
                } catch (MalformedURLException e) {
                    throw new RuntimeException(e);
                }
                String domain = url.getHost();

                try {
                    int index = (domain.startsWith("www.")) ? 1 : 0;
                    results.add(new ResultObject(urls.get(i), getTitle(urls.get(i)), mostMatchingParagraph, getIcon(urls.get(i)), domain.split("\\.")[index].substring(0, 1).toUpperCase() + domain.split("\\.")[index].substring(1)));
                } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                }
            } else break;
        }
        System.out.println("FinishedQuery at :" + LocalDateTime.now());
        return results;
    }
}