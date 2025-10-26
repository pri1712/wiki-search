package com.pri1712.searchengine.wikisearchApp;

import com.pri1712.searchengine.wikiparser.Parser;
import com.pri1712.searchengine.wikitokenizer.Tokenizer;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main {
    private static final Logger LOGGER = Logger.getLogger(Main.class.getName());
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        System.out.print("Enter path to Wikipedia XML dump file: ");
        String filePath = scanner.nextLine().trim();
        scanner.close();
        long startTime = System.nanoTime();
        try {
            Parser parser = new Parser(filePath);
            parser.parseData();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        try {
            Tokenizer tokenizer = new Tokenizer();
            String parsedFilePath = "data/parsed-data/";
            LOGGER.info("Parsing Wikipedia XML dump file: " + parsedFilePath);
            tokenizer.tokenizeData(parsedFilePath);

        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING, e.getMessage());
        }
        long endTime = System.nanoTime();
        long elapsedTime = endTime - startTime;
        LOGGER.log(Level.INFO,"Time taken to parse the data : {0} ms",elapsedTime/100000);
        LOGGER.log(Level.INFO,"Memory used: {0} MB", (Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory())/(1024*1024));
    }
}