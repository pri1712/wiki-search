package com.pri1712.searchengine.wikisearchApp;

import com.pri1712.searchengine.indexreader.IndexData;
import com.pri1712.searchengine.parser.Parser;
import com.pri1712.searchengine.tokenizer.Tokenizer;
import com.pri1712.searchengine.indexwriter.IndexWriter;
import com.pri1712.searchengine.indexreader.IndexReader;

import java.io.IOException;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());
    private static final String PARSED_FILE_PATH = "data/parsed-data/";
    private static final String TOKENIZED_FILE_PATH = "data/tokenized-data/";
    private static final String INDEXED_FILE_PATH = "data/inverted-index/";
    private static final String TOKEN_INDEX_OFFSET_PATH = "data/inverted-index/token_index_offset.json.gz";
    private static final String TEST_TOKEN = "drill";
    static String parsedFilePath = PARSED_FILE_PATH;
    static String tokenizedFilePath = TOKENIZED_FILE_PATH;
    static String indexedFilePath = INDEXED_FILE_PATH;
    static String tokenIndexOffsetPath = TOKEN_INDEX_OFFSET_PATH;

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter path to Wikipedia XML dump file: ");
        String dataFilePath = scanner.nextLine().trim();
        scanner.close();
        long startTime = System.nanoTime();
        //parser
        try {
            Parser parser = new Parser(dataFilePath);
            parser.parseData();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        //tokenizer
        try {
            Tokenizer tokenizer = new Tokenizer();
            LOGGER.info("Tokenizing Wikipedia XML dump file: " + parsedFilePath);
            tokenizer.tokenizeData(parsedFilePath);

        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING, e.getMessage());
            throw new RuntimeException(e);
        }
        //indexer
        try {
            IndexWriter indexWriter = new IndexWriter(indexedFilePath);
            LOGGER.info("Indexing Wikipedia XML dump file: " + tokenizedFilePath);
            indexWriter.indexData(tokenizedFilePath);
            indexWriter.mergeAllIndexes(indexedFilePath);
        } catch (RuntimeException | IOException e) {
            throw new RuntimeException(e);
        }

        //querying is the next step.
        try {
            IndexReader indexReader = new IndexReader(indexedFilePath,tokenIndexOffsetPath);
            IndexData indexData = indexReader.readTokenIndex(TEST_TOKEN);
            LOGGER.fine("Read data from inverted index for token " + TEST_TOKEN);
            LOGGER.fine("DocIds " + indexData.getDocIds());
            LOGGER.fine("frequencies " + indexData.getFreqs());
        } catch (RuntimeException | IOException e) {
            LOGGER.log(Level.WARNING, e.getMessage());
            throw new RuntimeException(e);
        }
        long endTime = System.nanoTime();
        long elapsedTime = endTime - startTime;
        LOGGER.log(Level.INFO,"Time taken to parse the data : {0} ms",elapsedTime/100000);
        LOGGER.log(Level.INFO,"Memory used: {0} MB", (Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory())/(1024*1024));


    }
}