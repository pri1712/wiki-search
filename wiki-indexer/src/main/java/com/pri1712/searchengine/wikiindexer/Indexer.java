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
import java.util.Map;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Indexer {
    private static Logger LOGGER = Logger.getLogger(String.valueOf(Indexer.class));
    ObjectMapper mapper = new ObjectMapper();
    private static int indexFileCounter = 0;
    private static final int MAX_IN_MEMORY_LENGTH = 10000;
    private final BatchFileWriter batchFileWriter = new BatchFileWriter("data/indexed-data/");
    Map<String, Map<Integer,Integer>> invertedIndex = new TreeMap<>();
    private static final int MAX_FILE_STREAM = 30;

    public Indexer() {
        //figure out how to do checkpointing here, it cant be as simple as the parser and tokenizer.
        //maybe we can compare the number of lines processed but that is a very simple way to do it especially~
        // if we wanna have memory based flushing
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

    public void mergeAllIndexes(String filePath) throws IOException {
        Path indexedPath = Paths.get(filePath);
        int indexRound = 0;
        List<Path> indexFiles = Files.list(indexedPath).filter(f -> f.toString().endsWith(".json.gz"))
                                .toList();
        while (indexFiles.size() > 1) {
            //till we have only one index.
            List<Path> nextRoundIndexes = new ArrayList<>();
            for (int i =0; i<indexFiles.size();i+=MAX_FILE_STREAM) {
                List<Path> batch = indexFiles.subList(i, Math.min(i+MAX_FILE_STREAM, indexFiles.size()));
                Path outputPath = indexedPath.resolve(String.format("merged_index%d_%03d.json.gz", indexRound, i / MAX_FILE_STREAM));
                mergeBatch(batch, outputPath);
                nextRoundIndexes.add(outputPath);

                for (Path p : batch) Files.deleteIfExists(p);
            }
            indexFiles = nextRoundIndexes;
            indexRound++;
        }

    }

    private void mergeBatch(List<Path> batch, Path outputPath) throws IOException {
        //actual file merging logic.
        PriorityQueue<HeapEntry> heap = new PriorityQueue<>(Comparator.comparing(heapEntry -> heapEntry.token));
        List<HeapEntry> entries = new ArrayList<>();
        for (Path p : batch) {
            FileInputStream fis = new FileInputStream(p.toFile());
            GZIPInputStream gis = new GZIPInputStream(fis);
            BufferedReader br = new BufferedReader(new InputStreamReader(gis));
            String line = br.readLine();
            if (line != null) {
                //create heapentry obj.
                Map <String,Map<Integer,Integer>> keyValueIndex = mapper.readValue(line, new TypeReference<>() {});
                String token = keyValueIndex.keySet().iterator().next();
                Map<Integer,Integer> docFreqMap = keyValueIndex.get(token);
                HeapEntry heapEntry = new HeapEntry(token, docFreqMap, br);
                entries.add(heapEntry);
            }
        }

        heap.addAll(entries);
        GZIPOutputStream gos  = new GZIPOutputStream(new FileOutputStream(outputPath.toFile()));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(gos));
        while (!heap.isEmpty()) {
            HeapEntry heapEntry = heap.poll();
            //now find all the ones with the same token.
            String token = heapEntry.token;
            Map<Integer,Integer> docFreqMap = new HashMap<>(heapEntry.docFreq);
            while (!heap.isEmpty() && heap.peek().token.equals(token)) {
                HeapEntry matchingEntry = heap.poll();
                matchingEntry.docFreq.forEach((doc, freq) -> docFreqMap.merge(doc, freq, Integer::sum));
                nextLine(matchingEntry,heap,mapper);
            }

        }
    }

    private void nextLine(HeapEntry heapEntry, PriorityQueue<HeapEntry> heap, ObjectMapper mapper) throws IOException {
        String nextLine = heapEntry.reader.readLine();
        if (nextLine == null) {
            LOGGER.log(Level.INFO, "Reached end of file");
            return;
        }
        Map<String,Map<Integer,Integer>> keyValueIndex = mapper.readValue(nextLine, new TypeReference<>() {});
        String token = keyValueIndex.keySet().iterator().next();
        Map<Integer,Integer> docFreqMap = keyValueIndex.get(token);
        HeapEntry nextHeapEntry = new HeapEntry(token, docFreqMap, heapEntry.reader);
        heap.add(nextHeapEntry);
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
