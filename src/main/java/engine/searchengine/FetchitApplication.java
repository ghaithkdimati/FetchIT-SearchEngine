package engine.searchengine;

import engine.searchengine.core.Crawler;
import engine.searchengine.core.CrawlerWrapper;
import engine.searchengine.core.IndexerWrapper;
import engine.searchengine.core.PageRank;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cglib.core.Local;

import java.time.LocalDateTime;
import java.util.logging.Level;
import java.util.logging.Logger;


@SpringBootApplication
public class FetchitApplication {

	public static void main(String[] args) {

		Logger.getLogger("org.mongodb.driver").setLevel(Level.OFF);

//		System.out.print("Crawler Started At: " + LocalDateTime.now());
//
//		new CrawlerWrapper();
//
//		System.out.println("Crawler Finished At: " + LocalDateTime.now());
//
//
//		System.out.print("Indexer Started : ");
//		System.out.println(LocalDateTime.now());
//
//		new IndexerWrapper();
//
//		System.out.print("Indexer Finished : ");
//		System.out.println(LocalDateTime.now());
//
//
//		System.out.print("PageRank Started : ");
//		System.out.println(LocalDateTime.now());
//		PageRank pr = new PageRank(Crawler.getNetworkGraph());
//
//		pr.run();
//
//		System.out.print("PageRank Finished : ");
//		System.out.println(LocalDateTime.now());

		SpringApplication.run(FetchitApplication.class, args);
	}
}