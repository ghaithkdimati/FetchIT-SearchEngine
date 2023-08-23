package engine.searchengine.core;

import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.bson.conversions.Bson;
import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import javax.print.Doc;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.logging.Filter;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.eq;

public class QueryProcessor {
    String Query;
    MongoDatabase database;
    MongoClient mongoClient;
    private MongoCollection<org.bson.Document> collection, DocsCollection;
    HashMap<String,Double> docRelevanceScore=new HashMap<String,Double>();

    HashMap<String,String> occurencesMap =new HashMap<String,String>();
    HashMap<String,Double> tagsWeights=new HashMap<String,Double>();
    org.bson.Document doc;
    public QueryProcessor()
    {
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:27017");
        database = mongoClient.getDatabase("SearchEngine");
        collection = database.getCollection("InvertedFile");
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

    public void PhraseSearch(String searchQuery) throws IOException, NoSuchAlgorithmException
    {
        searchQuery=searchQuery.toLowerCase(Locale.ROOT);
        String[] searchQueryArr=searchQuery.split(" ");
        Bson projectionFields = Projections.fields(
                Projections.include("token","DF","Docs"),
                Projections.excludeId());
        List<org.bson.Document> pages;
        docRelevanceScore = new HashMap<String,Double>();
        occurencesMap =new HashMap<String,String>();
        //doc=collection.find(eq("token","abdallah")).projection(projectionFields).first();
        //pages=(List<org.bson.Document>)doc.get("Docs");
        //System.out.println(doc);
//        System.out.println(searchQuery);
//        System.out.println(searchQueryArr[0]);
        for (String token:searchQueryArr)
        {
            Map<String,Boolean> visitedPage;
            doc=collection.find(eq("token",token)).projection(projectionFields).first();
//            System.out.println(doc);
            if(doc==null)
                continue;
            pages=doc.get("Docs",List.class);
            double DF=Double.parseDouble(doc.get("DF").toString());
            // System.out.println(DF);
            double TF;
            double IDF=DF;
            double totalTagWeight=0;
            //iterating on Docs that contain token
//            System.out.println(pages);
            for (int i = 0; i < pages.size(); i++)
            {
                //getting tag weight default value is of <p>
                String tag=pages.get(i).get("tag").toString();
                double tagWeight=(tagsWeights.containsKey(tag))?tagsWeights.get(tag):tagsWeights.get("p");
                //getting the page getting processed
                TF=Double.parseDouble(pages.get(i).get("TF").toString());
                String currentPage=pages.get(i).get("page").toString();
                String occurrence=pages.get(i).get("occurrence").toString();


                if(occurrence.contains(searchQuery))
                    if(docRelevanceScore.containsKey(currentPage))
                        docRelevanceScore.put(currentPage,docRelevanceScore.get(currentPage)+TF*IDF*tagWeight);
                    else
                        docRelevanceScore.put(currentPage,TF*IDF*tagWeight);
                if(!occurencesMap.containsKey(Query + "," + currentPage) && occurrence.contains(searchQuery))
                    occurencesMap.put(Query + "," + currentPage, occurrence);
                else if(occurrence.contains(searchQuery) && occurencesMap.get(Query + "," + currentPage).length() < 200)
                    if (!occurencesMap.get(Query + "," + currentPage).contains(searchQuery))
                        occurencesMap.put(Query + "," + currentPage, occurencesMap.get(Query + "," + currentPage) + " ... " + occurrence);
            }

        }

        this.occurencesMap = occurencesMap;

    }

    public HashMap<String,Double> getDocRelevanceScore()
    {
        docRelevanceScore=docRelevanceScore.entrySet()
                .stream()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e1, LinkedHashMap::new));;
        return docRelevanceScore;
    }
    public HashMap<String,String> getOccurencesMap()
    {
        return occurencesMap;
    }
    public void calculatePopularity()
    {
        MongoCollection<org.bson.Document> pageRankCollection=database.getCollection("PageRank");
        Bson pageRankProjectionFields=Projections.fields(Projections.include("url","score"),Projections.excludeId());

        for(Map.Entry<String,Double> page:docRelevanceScore.entrySet())
        {
            System.out.println("/////////////////////////");
            double pageRank=0.0;
            org.bson.Document pageRankDoc = pageRankCollection.find(eq("url",page.getKey())).first();
            if(pageRankDoc!=null)
                pageRank=Double.parseDouble(pageRankDoc.get("score").toString());
            System.out.println(page.getKey()+"          "+pageRank);
            docRelevanceScore.put(page.getKey(),0.8*page.getValue()+0.2*pageRank);
        }
    }
    public void andPhrases(String firstPhrase, String secondPhrase){
        HashMap<String, Double> firstPhraseMap = new HashMap<String, Double>();
        HashMap<String, Double> secondPhraseMap = new HashMap<String, Double>();
        HashMap<String, Double> result = new HashMap<String, Double>();
        HashMap<String, String> firstOccurrencesMap = new HashMap<String, String>();
        HashMap<String, String> secondOccurrencesMap = new HashMap<String, String>();
        HashMap<String, String> resultOccurrencesMap = new HashMap<String, String>();

        try {
            PhraseSearch(firstPhrase);
            firstPhraseMap = new HashMap<>(getDocRelevanceScore());
            firstOccurrencesMap = new HashMap<>(getOccurencesMap());
            PhraseSearch(secondPhrase);
            secondPhraseMap = new HashMap<>(getDocRelevanceScore());
            secondOccurrencesMap = new HashMap<>(getOccurencesMap());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        for (String key : firstPhraseMap.keySet()) {
            if (secondPhraseMap.containsKey(key)) {
                result.put(key, firstPhraseMap.get(key) + secondPhraseMap.get(key));
                resultOccurrencesMap.put(Query + "," + key, firstOccurrencesMap.get(Query + "," + key) + " " + secondOccurrencesMap.get(Query + "," + key));
            }
        }
        System.out.println(result);
        System.out.println(resultOccurrencesMap);
        docRelevanceScore = result;
        this.occurencesMap = resultOccurrencesMap;
    }

    public void andPhrasesOnList(String secondPhrase){
        HashMap<String,Double> docMap = new HashMap<String, Double>(getDocRelevanceScore());
        HashMap<String,String> occurencesMap = new HashMap<String, String>(getOccurencesMap());
        HashMap<String, Double> secondPhraseMap = new HashMap<String, Double>();
        HashMap<String, Double> result = new HashMap<String, Double>();
        HashMap<String, String> secondOccurrencesMap = new HashMap<String, String>();
        HashMap<String, String> resultOccurrencesMap = new HashMap<String, String>();

        try {
            PhraseSearch(secondPhrase);
            secondPhraseMap = new HashMap<>(getDocRelevanceScore());
            secondOccurrencesMap = new HashMap<>(getOccurencesMap());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        for (String key : docMap.keySet()) {
            if (secondPhraseMap.containsKey(key)) {
                result.put(key, docMap.get(key) + secondPhraseMap.get(key));
                resultOccurrencesMap.put(Query + "," + key, occurencesMap.get(Query + "," + key) + " " + secondOccurrencesMap.get(Query + "," + key));
            }
        }
        docRelevanceScore = result;
        this.occurencesMap = resultOccurrencesMap;
    }

    public void orPhrasesOnList(String secondPhrase){
        HashMap<String,Double> docMap = new HashMap<String, Double>(getDocRelevanceScore());
        HashMap<String,String> occurencesMap = new HashMap<String, String>(getOccurencesMap());
        HashMap<String, Double> secondPhraseMap = new HashMap<String, Double>();
        HashMap<String, Double> result = new HashMap<String, Double>();
        HashMap<String, String> secondOccurrencesMap = new HashMap<String, String>();
        HashMap<String, String> resultOccurrencesMap = new HashMap<String, String>();

        try {
            PhraseSearch(secondPhrase);
            secondPhraseMap = new HashMap<>(getDocRelevanceScore());
            secondOccurrencesMap = new HashMap<>(getOccurencesMap());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        for (String key : docMap.keySet()) {
            if (secondPhraseMap.containsKey(key)) {
                result.put(key, docMap.get(key) + secondPhraseMap.get(key));
                resultOccurrencesMap.put(Query + "," + key, occurencesMap.get(Query + "," + key) + " " + secondOccurrencesMap.get(Query + "," + key));
            }else{
                result.put(key, docMap.get(key));
                resultOccurrencesMap.put(Query + "," + key, occurencesMap.get(Query + "," + key));
            }
        }

        for(String key : secondPhraseMap.keySet()){
            if(!result.containsKey(key)){
                result.put(key, secondPhraseMap.get(key));
                resultOccurrencesMap.put(Query + "," + key, secondOccurrencesMap.get(key));
            }
        }

        docRelevanceScore = result;
        this.occurencesMap = resultOccurrencesMap;
    }

    public void orPhrases(String firstPhrase,String secondPhrase){
        HashMap<String, Double> firstPhraseMap = new HashMap<String, Double>();
        HashMap<String, Double> secondPhraseMap = new HashMap<String, Double>();
        HashMap<String, Double> result = new HashMap<String, Double>();
        HashMap<String, String> firstOccurrencesMap = new HashMap<String, String>();
        HashMap<String, String> secondOccurrencesMap = new HashMap<String, String>();
        HashMap<String, String> resultOccurrencesMap = new HashMap<String, String>();

        try {
            PhraseSearch(firstPhrase);
            firstPhraseMap = new HashMap<>(getDocRelevanceScore());
            firstOccurrencesMap = new HashMap<>(getOccurencesMap());
            PhraseSearch(secondPhrase);
            secondPhraseMap = new HashMap<>(getDocRelevanceScore());
            secondOccurrencesMap = new HashMap<>(getOccurencesMap());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        for (String key : firstPhraseMap.keySet()) {
            if (secondPhraseMap.containsKey(key)) {
                result.put(key, firstPhraseMap.get(key) + secondPhraseMap.get(key));
                resultOccurrencesMap.put(Query + "," + key, firstOccurrencesMap.get(Query + "," + key) + " " + secondOccurrencesMap.get(Query + "," + key));
            }
        }
        for (String key : firstPhraseMap.keySet()) {
            if (!result.containsKey(key)) {
                result.put(key, firstPhraseMap.get(key));
                resultOccurrencesMap.put(Query + "," + key, firstOccurrencesMap.get(Query + "," + key));
            }
        }
        for (String key : secondPhraseMap.keySet()) {
            if (!result.containsKey(key)) {
                result.put(key, secondPhraseMap.get(key));
                resultOccurrencesMap.put(Query + "," + key, secondOccurrencesMap.get(Query + "," + key));
            }
        }
        docRelevanceScore = result;
        this.occurencesMap = resultOccurrencesMap;
    }

    public void notPhrase(String searchPhrase){
        HashMap<String,Double> docMap = new HashMap<String, Double>(getDocRelevanceScore());
        HashMap<String,String> occurencesMap = new HashMap<String, String>(getOccurencesMap());
        Map<String, Double> resultPhraseMap = new HashMap<String, Double>();
        try {
            this.PhraseSearch(searchPhrase);
            resultPhraseMap = new HashMap<>(getDocRelevanceScore());
            System.out.println(resultPhraseMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        for (String key : resultPhraseMap.keySet()) {
            if (docMap.containsKey(key)) {
                docMap.remove(key);
                occurencesMap.remove(Query + "," + key);
            }
        }
        this.docRelevanceScore = docMap;
        this.occurencesMap = occurencesMap;
    }

    public void processQuery(String searchQuery){
        searchQuery = searchQuery.toLowerCase();
        this.Query = searchQuery;
        System.out.println("Query: " + Query);
        System.out.println("sQuery: " +searchQuery);
        List<String> phrases = new ArrayList<String>();
        List<String> operators = new ArrayList<String>();
        phrases = extractPhrases(searchQuery);
        operators = extractOperators(searchQuery);

        for (int i = 0; i < phrases.size(); i++) {
            System.out.println(phrases.get(i));
        }
        for (int i = 0; i < operators.size(); i++) {
            System.out.println(operators.get(i));
        }

        if (phrases.size() == 1 || !(phrases.size() == operators.size() + 1)) {
            try {
                PhraseSearch(phrases.get(0));

                calculatePopularity();
                return;
            } catch (IOException e) {
                e.printStackTrace();
            } catch (NoSuchAlgorithmException e) {
                e.printStackTrace();
            }
        }


        if(operators.get(0).equals("and")){
            int i = operators.indexOf("and");

            andPhrases(phrases.get(i),phrases.get(i+1));


            operators.remove(i);

            if (operators.size() != 0 && operators.get(0).equals("and")) {
                andPhrasesOnList(phrases.get(i+2));
            }else if (operators.size() != 0 &&operators.get(0).equals("or")) {
                orPhrasesOnList(phrases.get(i+2));
            } else if (operators.size() != 0 &&operators.get(0).equals("not")) {
                notPhrase(phrases.get(i+2));
            }
        }else if(operators.get(0).equals("or")){
            if(operators.size() > 1 && operators.get(1).equals("and")) {
                andPhrases(phrases.get(1), phrases.get(2));
                orPhrasesOnList(phrases.get(0));
            }else{
                orPhrases(phrases.get(0), phrases.get(1));
                if (operators.size() > 1)
                    orPhrasesOnList(phrases.get(2));
            }
        }else if(operators.get(0).equals("not")){
            try {
                PhraseSearch(phrases.get(0));
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }
            notPhrase(phrases.get(1));
            if (operators.size() > 1 && operators.get(1).equals("and")) {
                andPhrasesOnList(phrases.get(2));
            }
        }

        calculatePopularity();

    }

    public static ArrayList<String> extractPhrases(String query) {
        ArrayList<String> phrases = new ArrayList<>();
        int i = 0;
        while (i < query.length()) {
            if (query.charAt(i) == '"') {
                int j = i + 1;
                while (j < query.length() && query.charAt(j) != '"') {
                    j++;
                }
                String phrase = query.substring(i+1, j);
                phrases.add(phrase);
                i = j + 1;
            } else {
                i++;
            }
        }
        return phrases;
    }

    public static ArrayList<String> extractOperators(String query) {
        ArrayList<String> operators = new ArrayList<>();
        ArrayList<String> operands = extractPhrases(query);
        int min = query.length();
        for (int i = 0; i < operands.size(); i++) {
            int j = query.indexOf(operands.get(i)) + operands.get(i).length();
            if (j < query.length()) {
                int andIdx = query.indexOf("and", j);
                int notIdx = query.indexOf("not", j);
                int orIdx = query.indexOf("or", j);
                String operator = "";
                // get min of andIdx, notIdx, orIdx
                min = query.length();
                if (andIdx != -1 && andIdx < min) {
                    min = andIdx;
                    operator = "and";
                }
                if (notIdx != -1 && notIdx < min) {
                    min = notIdx;
                    operator = "not";
                }
                if (orIdx != -1 && orIdx < min) {
                    min = orIdx;
                    operator = "or";
                }

                if (operator != "") {
                    operators.add(operator);
                }
            }
            query = query.substring(min);
        }
        return operators;
    }

}
