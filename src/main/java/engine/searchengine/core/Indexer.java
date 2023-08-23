package engine.searchengine.core;

import com.mongodb.client.*;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.tartarus.snowball.ext.EnglishStemmer;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.CountDownLatch;


public class Indexer {
    private CountDownLatch latch;

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
    private static long documentsIndexed = 0; // Documents Indexed so far
    private final float MAX_TF = 0.2f;

    private final MongoCollection<org.bson.Document> collection, DocsCollection, ImgCollection;
    /**
     * TF-IDF = TF * IDF
     *        = (Number of times the word appears in the document) / (Total number of words in the document) * log(Total number of documents / Number of documents with the word)
     */
    private final MongoClient mongoClient;
    private final MongoDatabase database;

    private final HashMap<String, String> tokensID;
    public Indexer(CountDownLatch latch, MongoClient mongoClient, MongoDatabase database
                    , MongoCollection<org.bson.Document> collection, MongoCollection<org.bson.Document> DocsCollection, MongoCollection<org.bson.Document> ImgCollection,
            HashMap<String, String> tokensID) {
        // Connect to the database
        this.mongoClient = mongoClient;
        this.database = database;
        this.collection = collection;
        this.DocsCollection = DocsCollection;
        this.ImgCollection = ImgCollection;
        this.tokensID = tokensID;
        this.latch = latch;

        if(collection.countDocuments() > 0){
            MongoCursor<org.bson.Document> cursor = collection.find().iterator();
            // Loop over the collection to insert each document into the HashMap
            while (cursor.hasNext()) {
                // Get the document and extract the key-value pairs
                org.bson.Document document = cursor.next();
                String _id = document.get("_id").toString();
                String token = document.getString("token");
                tokensID.put(token,_id);
            }
        }



    }


    public void index(String url, String filepath, FetchType type) {

        try {

            /**
             * Tokens Check if the document is already indexed or not
             * So it sets the database ready for updating the data
             */
            Bson updateTokensFilter = Filters.eq("URL", url);
            org.bson.Document tokenPage = DocsCollection.find(updateTokensFilter).first();
            Bson documentFilter = Filters.eq("page", url);
            if(tokenPage != null) {
                collection.updateMany(new org.bson.Document(), Updates.pull("Docs", documentFilter));
                collection.updateMany(new org.bson.Document(), Updates.pull("Unique_Docs", url));
            }

            Document doc;
            long documentExistsFilter = DocsCollection.countDocuments(Filters.eq("URL", url));
            if(documentExistsFilter == 0) {
                if(type == FetchType.FILE) {
                    // This is the file object needed to parse
                    // Temporarily using a file object, I'll use Jsoup.connect() later
                    File htmlFile = new File(filepath);
                    org.bson.Document DocumentDoc = new org.bson.Document("URL", url);
                    DocumentDoc.put("Extension", "html");
                    DocumentDoc.put("Hash", Fingerprint.getHTMLHash(htmlFile));
                    DocsCollection.insertOne(DocumentDoc);
                    // parsing the HTML file
                    doc = Jsoup.parse(htmlFile, "UTF-8");
                } else {
                    URL uri = new URL(url);
                    org.bson.Document DocumentDoc = new org.bson.Document("URL", uri.toString());
                    String[] urlParts = uri.getHost().split("\\.");
                    DocumentDoc.put("Extension", urlParts[urlParts.length - 1]);
                    DocumentDoc.put("Hash", Fingerprint.getPageHash(url.toString()));
                    DocsCollection.insertOne(DocumentDoc);
                    try{
                        doc = Jsoup.connect(filepath).get();
                    } catch (IOException e) {
                        e.printStackTrace();

                        latch.countDown(); // signal that this thread is done
                        return;
                    }
                    catch (Exception e) {

                        latch.countDown(); // signal that this thread is done
                        return;
                    }
                }
            }
            else{
                if(type == FetchType.FILE) {

                    String filePathHash = DocsCollection.find(Filters.eq("URL", url)).first().get("Hash", String.class);

                    // This is the file object needed to parse
                    // Temporarily using a file object, I'll use Jsoup.connect() later
                    File htmlFile = new File(filepath);
                    // parsing the HTML file
                    doc = Jsoup.parse(htmlFile, "UTF-8");
                    System.out.println(filePathHash);
                    System.out.println(Fingerprint.getHTMLHash(htmlFile));
                    if(filePathHash.equals(Fingerprint.getHTMLHash(htmlFile))){
                        latch.countDown(); // signal that this thread is done
                        return;
                    }
                    DocsCollection.updateOne(Filters.eq("URL", url), Updates.set("Hash", Fingerprint.getHTMLHash(htmlFile)));

                }
                else {
                    String filePathHash = DocsCollection.find(Filters.eq("URL", url)).first().get("Hash", String.class);

                    try{
                        doc = Jsoup.connect(filepath).get();
                    } catch (IOException e) {
                        e.printStackTrace();

                        latch.countDown(); // signal that this thread is done
                        return;
                    }
                    if(filePathHash.equals(Fingerprint.getPageHash(filepath))) {

                        latch.countDown(); // signal that this thread is done
                        return;
                    }
                    DocsCollection.updateOne(Filters.eq("URL", filepath), Updates.set("Hash", Fingerprint.getPageHash(filepath)));
                    System.out.println(filePathHash);
                    System.out.println(Fingerprint.getPageHash(filepath));
                }
            }




            // Calculating the document length
            int documentLength = doc.select("title, body").text().split("[^a-zA-Z0-9']+").length;
//            System.out.println("Document Length: " + documentLength);

            // Get all the elements in the document with the specified tags
            Elements imageElems  = doc.select("img");
            for (Element img: imageElems){
                String alt = img.attr("alt");
                String src = img.attr("src");
                if(src.length() < 4 || !src.substring(0,4).equals("http")
                        || ImgCollection.countDocuments(Filters.eq("url", src)) > 0)
                    continue;
                org.bson.Document imageDoc = new org.bson.Document();
                imageDoc.put("url", src);
                imageDoc.put("alt", alt);
                imageDoc.put("page", url);
                ImgCollection.insertOne(imageDoc);
            }

            Elements elems  = doc.select("label, title, p, h1, h2, h3, h4, h5, h6");
            // Creating a list of tokens
            // It's a filteration stage
            List<String> tokens = new ArrayList<>();
            for (Element elem : elems) {
                // Element Basic Data (Text, Tag)
                /**
                 * Example:
                 *      <p>Abdallah Ahmed</p>
                 *      >> Text: Abdallah Ahmed
                 *      >> Tag: p
                 *
                 * Splitting the Text into words
                 *      >> TagWords: [Abdallah, Ahmed]
                 */
                String Text = elem.text();
                String Tag = elem.tagName();
                String[] TagWords = Text.split("[^a-zA-Z0-9']+");
                // Looping on the words in the TagWords
                for (String word : TagWords) {
                    // Getting Word, Tag, TF from the token
//                    System.out.println("Stemmed Word: " + stemmedWord);
                    // Verify it's a valid word
                    if(word.length() > 0 && !stopwords.contains(word.toLowerCase())){
                        String tokenWord = word.toLowerCase();
                        EnglishStemmer stemmer = new EnglishStemmer();
                        stemmer.setCurrent(tokenWord);
                        String stemmedWord = tokenWord;
                        if(stemmer.stem()){
                            stemmedWord = stemmer.getCurrent();
                        }
                        String finalStem = stemmedWord;
                        // Verify it's not a stop word
                        // Verify it's not already in the tokens list
                        // Calculate the TF
                        int TF = Arrays.stream(elems.text().split("[^a-zA-Z0-9']+"))
                                .filter(s -> s.equalsIgnoreCase(finalStem)).toArray().length;
                        if(TF/documentLength < MAX_TF && (double)TF/documentLength > 0){
                            // Add the token to the tokens list
                            // Token Format: word,tag,TF
                            // Getting the file name with extension (e.g. file.txt) it's by default the second element in the filepath
                            // In case the filepath is a URL, we'll use the URL as the file name without splitting
                            String fileNameWithExt = url;

                            String tokenTag = Tag;
                            float tokenTF = (((float)TF/documentLength));


                            synchronized (collection){
                                if(!tokensID.containsKey(stemmedWord)){ // collection.countDocuments(Filters.eq("token",stemmedWord)) == 0
                                    org.bson.Document tokenDoc = new org.bson.Document();
                                    tokenDoc.put("token", stemmedWord);
                                    tokenDoc.put("DF", 1);
                                    tokenDoc.put("Docs", new ArrayList<>());
                                    tokenDoc.put("Unique_Docs", new ArrayList<>());
                                    collection.insertOne(tokenDoc);
                                    tokensID.put(stemmedWord,tokenDoc.get("_id").toString());
                                }
                            }
                            int DF = 0;

                            boolean documentFound = false;

                            org.bson.Document PageDocument = new org.bson.Document();
                            PageDocument.put("originalWord", tokenWord);
                            PageDocument.put("page", fileNameWithExt);
                            PageDocument.put("tag", tokenTag);
                            PageDocument.put("TF", tokenTF);
                            PageDocument.put("occurrence", Text.toLowerCase());

                            Bson filter = Filters.eq("_id", new ObjectId(tokensID.get(stemmedWord)));
                            Bson updateCriteria = Updates.combine(
                                    Updates.push("Docs", PageDocument),
                                    Updates.addToSet("Unique_Docs", url)
                            );
                            collection.updateOne(filter, updateCriteria);

                        }
                    }
                }

            }

            latch.countDown(); // signal that this thread is done
            System.out.println("Latch value: " + latch.getCount());
        }catch (IOException e) {
            e.printStackTrace();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public void UpdateIDF(){
        // Find all documents in the collection
        FindIterable<org.bson.Document> cursor = collection.find();
        documentsIndexed = DocsCollection.countDocuments();
        // Loop through the cursor to print the value of the "name" field in each document
        for (org.bson.Document doc : cursor) {
            String token = doc.getString("token");

            double DF = doc.get("Unique_Docs", List.class).size();
            double IDF = Math.log10(documentsIndexed/DF);
            System.out.println("Token: " + token + " DF: " + DF + "DocsNum : " + documentsIndexed + " IDF: " + IDF);

            collection.updateOne(
                    new org.bson.Document("_id", doc.getObjectId("_id")),
                    Updates.combine(
                    new org.bson.Document("$set", new org.bson.Document("IDF", IDF)),
                    new org.bson.Document("$set", new org.bson.Document("DF", DF))
                    )
            );
        }
    }


}