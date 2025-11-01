package com.pri1712.searchengine.wikiindexer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pri1712.searchengine.wikiutils.BatchFileWriter;
import com.pri1712.searchengine.wikitokenizer.TokenizedData;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

public class Indexer {
    private static Logger LOGGER = Logger.getLogger(String.valueOf(Indexer.class));
    ObjectMapper mapper = new ObjectMapper();
    private static int indexFileCounter = 0;
    private static final int MAX_IN_MEMORY_LENGTH = 10000;
    private final BatchFileWriter batchFileWriter = new BatchFileWriter("data/indexed-data/");
    Map<String, Map<Integer,Integer>> invertedIndex = new TreeMap<>();

    public Indexer() {
        //figure out how to do checkpointing here, it cant be as simple as the parser and tokenizer.
    }

    public void indexData(String filePath) throws IOException {
        Path tokenizedPath = Paths.get(filePath);
        try (Stream<Path> fileStream = Files.list(tokenizedPath).filter(f -> f.toString().endsWith(".json.gz"))) {
            fileStream.forEach(file -> {
                //actual indexing logic here.
                try {
                    addToIndex(file);
                } catch (Exception e) {
                    LOGGER.log(Level.SEVERE, "Error indexing file: " + file, e);
                }
            });
        } catch (RuntimeException e) {
            LOGGER.log(Level.WARNING, "Failed to read tokenized JSON file", e);
        }
    }

    private void addToIndex(Path file) throws FileNotFoundException {
        try {
            FileInputStream fis = new FileInputStream(file.toString());
            GZIPInputStream gis = new GZIPInputStream(fis);
            BufferedReader buffRead = new BufferedReader(new InputStreamReader(gis));
            List <TokenizedData> tokenizedDocuments = mapper.readValue(buffRead, new TypeReference<List<TokenizedData>>() {
            });
            for (TokenizedData document : tokenizedDocuments) {
                //process each json file of title,text,doc id here separately and add to index.
                addDocument(document);
                boolean flush = flushToDisk();
                if (flush) {
                    //sort all the keys and then flush to disk.

                    LOGGER.info("Flushing to disk");
                    batchFileWriter.writeIndex(invertedIndex,indexFileCounter);
                    invertedIndex.clear();
                    indexFileCounter++;
                    return;
                }
            }

        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Failed to read file: " + file.toString(), e);
        }
    }

    private void addDocument(TokenizedData doc) {
        List<String> text = doc.getTokenizedText();
        List<String> title = doc.getTokenizedTitle();
        String id = doc.getId();
        for (String token : text) {
            invertedIndex
                    .computeIfAbsent(token, k -> new HashMap<>())
                    .merge(Integer.parseInt(id), 1, Integer::sum);

        }
        for (String token : title) {
            invertedIndex
                    .computeIfAbsent(token, k -> new HashMap<>())
                    .merge(Integer.parseInt(id), 1, Integer::sum);
        }

    }

    private boolean flushToDisk() {
        //deciding whether to flush to disk or not.
        return invertedIndex.size() >= MAX_IN_MEMORY_LENGTH; //very rudimentary check, use heap size later
    }


}
