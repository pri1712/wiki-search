package com.pri1712.searchengine.wikisearchApp;

import com.pri1712.searchengine.wikiparser.Parser;
import com.pri1712.searchengine.wikitokenizer.Tokenizer;
import com.pri1712.searchengine.wikiindexer.Indexer;

import java.io.IOException;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());
    private static final String PARSED_FILE_PATH = "data/parsed-data/";
    private static final String TOKENIZED_FILE_PATH = "data/tokenized-data/";
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter path to Wikipedia XML dump file: ");
        String filePath = scanner.nextLine().trim();
        scanner.close();
        long startTime = System.nanoTime();
        //parser
        try {
            Parser parser = new Parser(filePath);
            parser.parseData();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        //tokenizer
        try {
            Tokenizer tokenizer = new Tokenizer();
            String parsedFilePath = PARSED_FILE_PATH;
            LOGGER.info("Tokenizing Wikipedia XML dump file: " + parsedFilePath);
            tokenizer.tokenizeData(parsedFilePath);

        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING, e.getMessage());
            throw new RuntimeException(e);
        }
        //indexer
        try {
            Indexer indexer = new Indexer();
            String tokenizedFilePath = TOKENIZED_FILE_PATH;
            LOGGER.info("Indexing Wikipedia XML dump file: " + tokenizedFilePath);
            indexer.indexData(tokenizedFilePath);
        } catch (RuntimeException | IOException e) {
            throw new RuntimeException(e);
        }
        long endTime = System.nanoTime();
        long elapsedTime = endTime - startTime;
        LOGGER.log(Level.INFO,"Time taken to parse the data : {0} ms",elapsedTime/100000);
        LOGGER.log(Level.INFO,"Memory used: {0} MB", (Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory())/(1024*1024));
    }
}