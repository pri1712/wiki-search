package com.pri1712.searchengine.wikisearchApp;

import com.pri1712.searchengine.indexreader.IndexData;
import com.pri1712.searchengine.parser.Parser;
import com.pri1712.searchengine.tokenizer.Tokenizer;
import com.pri1712.searchengine.indexwriter.IndexWriter;
import com.pri1712.searchengine.indexreader.IndexReader;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());
    private static final String PARSED_FILE_PATH = "data/parsed-data/";
    private static final String TOKENIZED_FILE_PATH = "data/tokenized-data/";
    private static final String INDEXED_FILE_PATH = "data/inverted-index/";
    private static final String TOKEN_INDEX_OFFSET_PATH = "data/inverted-index/token_index_offset.json.gz";
    private static final String DOC_STATS_PATH = "data/doc-stats/";

    private static final String TEST_TOKEN = "aaaaamaaj";
    static String parsedFilePath = PARSED_FILE_PATH;
    static String tokenizedFilePath = TOKENIZED_FILE_PATH;
    static String indexedFilePath = INDEXED_FILE_PATH;
    static String tokenIndexOffsetPath = TOKEN_INDEX_OFFSET_PATH;
    private static String READ_MODE = System.getenv("READ_MODE");
    public static void main(String[] args) throws IOException {
        long startTime = System.nanoTime();
        Map<String,String> parsedArgs = parseArgs(args);
        String mode = parsedArgs.getOrDefault("mode", "read");
        String dataPath = parsedArgs.get("data");
        indexedFilePath = parsedArgs.getOrDefault("index", indexedFilePath);
        if ("write".equalsIgnoreCase(mode)) {
            runWritePipeline(dataPath);
            return;
        }
        IndexReader indexReader = openIndexReader(indexedFilePath);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                LOGGER.info("Shutting down, closing index reader...");
                indexReader.close();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Failed closing index reader", e);
            }
        }));

        runReadPipeline(indexReader,indexedFilePath);
        long endTime = System.nanoTime();
        long elapsedTime = endTime - startTime;
        LOGGER.log(Level.INFO,"Time taken to parse the data : {0} ms",elapsedTime/100000);
        LOGGER.log(Level.INFO,"Memory used: {0} MB", (Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory())/(1024*1024));

    }
    private static Map<String,String> parseArgs(String[] args) {
        Map<String,String> parsedArgs = new HashMap<>();
        for (String a : args) {
            if (a.startsWith("--")) {
                String[] parts = a.substring(2).split("=", 2);
                parsedArgs.put(parts[0], parts.length > 1 ? parts[1] : "");
            }
        }
        return parsedArgs;
    }

    private static void runWritePipeline(String dataPath) {
        try {
            Parser parser = new Parser(dataPath);
            parser.parseData();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try {
            Tokenizer tokenizer = new Tokenizer();
            LOGGER.info("Tokenizing Wikipedia XML dump file: " + parsedFilePath);
            tokenizer.tokenizeData(parsedFilePath);

        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING, e.getMessage());
            throw new RuntimeException(e);
        }
        try {
            IndexWriter indexWriter = new IndexWriter(indexedFilePath);
            LOGGER.info("Indexing Wikipedia XML dump file: " + tokenizedFilePath);
            indexWriter.indexData(tokenizedFilePath);
            indexWriter.mergeAllIndexes(indexedFilePath);
        } catch (RuntimeException | IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void runReadPipeline(IndexReader indexReader, String indexedFilePath) {

        try (Scanner scanner = new Scanner(System.in)) {
            System.out.println("Ready for queries. Type ':reload' to reload index, ':exit' to quit.");
            while (true) {
                System.out.print("> ");
                if (!scanner.hasNextLine()) break;
                String line = scanner.nextLine().trim();
                if (line.isEmpty()) continue;
                if (line.equalsIgnoreCase(":exit")) break;

                if (line.equalsIgnoreCase(":reload")) {
                    try {
                        indexReader.close();
                        indexReader = openIndexReader(indexedFilePath); // reopen new reader instance
                        System.out.println("Index reloaded.");
                    } catch (IOException e) {
                        System.err.println("Reload failed: " + e.getMessage());
                    }
                    continue;
                }

                // run search — do not block reader creation; use executor if heavy
                try {
                    IndexData data = indexReader.readTokenIndex(line); // or indexReader.search(...)
                    System.out.println("DocIds: " + data.getDocIds());
                    System.out.println("Freqs:  " + data.getFreqs());
                } catch (Exception e) {
                    LOGGER.log(Level.WARNING, "Query failed", e);
                    System.out.println("Query error: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Read loop terminated unexpectedly", e);
        } finally {
            try { indexReader.close(); } catch (IOException ignore) {}
        }
    }

    private static IndexReader openIndexReader(String indexPath) throws IOException {
        Path indexedPath = Paths.get(indexPath);
        LOGGER.info("Opening index at " + indexedPath.toAbsolutePath());
        return new IndexReader(indexedPath.toString(),tokenIndexOffsetPath);
    }
}