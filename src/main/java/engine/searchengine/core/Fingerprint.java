package engine.searchengine.core;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
public class Fingerprint {
    // Define the hash algorithm to use
    private static final String HASH_ALGORITHM = "SHA-256";

    // Compute a hash of the page's text content
    public static String getPageHash(String url) throws IOException, NoSuchAlgorithmException {
        // Retrieve the HTML content of the page
        Document doc;
        try {
            doc = Jsoup.connect(url).get();
        }   catch (Exception e) {
            return "Error in URL from getPageHash";
        }
        String html = doc.html();

        // Remove all HTML tags and other non-text content
        String cleanText = Jsoup.parse(html).text();

        // Convert all text to lowercase and remove any leading or trailing white space
        cleanText = cleanText.toLowerCase().trim();

        // Compute the SHA-256 hash of the cleaned-up text
        MessageDigest digest = MessageDigest.getInstance(HASH_ALGORITHM);
        byte[] hashBytes = digest.digest(cleanText.getBytes(StandardCharsets.UTF_8));
        String hash = bytesToHex(hashBytes);

        return hash;
    }

    public static String getHTMLHash(File file) throws IOException, NoSuchAlgorithmException {
        // Retrieve the HTML content of the page
        Document doc;
        try {
            doc = Jsoup.parse(file, "UTF-8");
        }   catch (Exception e) {
            return "Error in URL from getPageHash";
        }

        // Remove all HTML tags and other non-text content
        String cleanText = doc.text();

        // Convert all text to lowercase and remove any leading or trailing white space
        cleanText = cleanText.toLowerCase().trim();

        // Compute the SHA-256 hash of the cleaned-up text
        MessageDigest digest = MessageDigest.getInstance(HASH_ALGORITHM);
        byte[] hashBytes = digest.digest(cleanText.getBytes(StandardCharsets.UTF_8));
        String hash = bytesToHex(hashBytes);

        return hash;
    }

    // Compute a hash of the page's DOM tree
    public String getDomHash(String url) throws NoSuchAlgorithmException, IOException {
        // Load the page using Jsoup
        Document doc = Jsoup.connect(url).get();

        // Remove scripts and styles from the document to avoid including them in the hash
        doc.select("script, style").remove();

        // Compute the SHA-256 hash of the DOM tree
        byte[] hashBytes = computeHash(doc);

        // Convert the hash bytes to a hex string
        String hash = bytesToHex(hashBytes);

        return hash;
    }

    // Helper method to compute the hash of an element and its children
    private byte[] computeHash(Element element) throws NoSuchAlgorithmException {
        // Create a MessageDigest object using the hash algorithm
        MessageDigest md = MessageDigest.getInstance(HASH_ALGORITHM);

        // Compute the hash of the element's tag name and attributes
        String tag = element.tagName().toLowerCase();
        String attrs = element.attributes().toString().toLowerCase();
        md.update((tag + attrs).getBytes(StandardCharsets.UTF_8));

        // Compute the hash of the text content of the element
        String text = element.text().toLowerCase();
        md.update(text.getBytes(StandardCharsets.UTF_8));

        // Compute the hash of the element's children, if any
        Elements children = element.children();
        for (Element child : children) {
            byte[] childHash = computeHash(child);
            md.update(childHash);
        }

        // Return the final hash value
        return md.digest();
    }

    // Helper method to convert a byte array to a hex string
    // { 0x12, 0x34, 0x56, (byte) 0x78 } becomes "12345678"
    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            // Format each byte as a two-digit hex number
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

}