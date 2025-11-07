package com.pri1712.searchengine.wikiindexer;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
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
    ObjectMapper mapper = new ObjectMapper().configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false)
            .configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, true);
    private static int indexFileCounter = 0;
    private static final int MAX_IN_MEMORY_LENGTH = 10000;
    private BatchFileWriter batchFileWriter;
    Map<String, Map<Integer,Integer>> invertedIndex = new TreeMap<>();
    private static final int MAX_FILE_STREAM = 10;
//    int temp_counter = 0;
    public Indexer(String indexedFilePath) {
        //figure out how to do checkpointing here, it cant be as simple as the parser and tokenizer.
        //maybe we can compare the number of lines processed but that is a very simple way to do it especially~
        // if we wanna have memory based flushing
        this.batchFileWriter = new BatchFileWriter(indexedFilePath);
    }

    public void indexData(String filePath) throws IOException {
        Path tokenizedPath = Paths.get(filePath);
        try (Stream<Path> fileStream = Files.list(tokenizedPath).filter(f -> f.toString().endsWith(".json.gz"))) {
            fileStream.forEach(file -> {
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

    //merge all the created inverted indexes.
    public void mergeAllIndexes(String filePath) throws IOException {
        Path indexedPath = Paths.get(filePath);
        int indexRound = 0;
        List<Path> indexFiles = Files.list(indexedPath)
                .filter(p -> {
                    String name = p.getFileName().toString();
                    return name.endsWith(".json.gz") && !name.startsWith("merged_");
                }).sorted().toList();
        //create a list of all the index files.
        while (indexFiles.size() > 1) {
            //till we have only one index file (final inverted index)
            LOGGER.log(Level.INFO,"index files size: {0}", indexFiles.size());
            List<Path> nextRoundIndexes = new ArrayList<>();
            for (int i =0; i<indexFiles.size(); i+=MAX_FILE_STREAM) {
                List<Path> batch = indexFiles.subList(i, Math.min(i+MAX_FILE_STREAM, indexFiles.size()));
                Path outputPath = indexedPath.resolve(String.format("merged_index%d_%03d.json.gz", indexRound, i / MAX_FILE_STREAM));
                LOGGER.info("Starting to merge indexed files; round " + indexRound);
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
            //basically read the first element of all the files part of batch.
            BufferedReader br;
            try {
                FileInputStream fis = new FileInputStream(p.toFile());
                GZIPInputStream gis = new GZIPInputStream(fis);
                br = new BufferedReader(new InputStreamReader(gis));
                String line = br.readLine();
//                LOGGER.log(Level.INFO, "Processing line " + line + " from file " + p);
                if (line != null) {
                    //create heapentry obj.
                    Map <String,Map<Integer,Integer>> keyValueIndex = mapper.readValue(line, new TypeReference<>() {});
                    String token = keyValueIndex.keySet().iterator().next();
                    Map<Integer,Integer> docFreqMap = keyValueIndex.get(token);
                    HeapEntry heapEntry = new HeapEntry(token, docFreqMap, br);
//                    LOGGER.info("adding token:" + heapEntry.token + " to entries array");
                    entries.add(heapEntry);
                }
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "Error reading file " + p, e);
            }
        }
//        LOGGER.info("Processing " + entries.size() + " entries");
        heap.addAll(entries);
//        LOGGER.info("Creating a gzip o/p stream");
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
            mapper.writeValue(bw, Map.of(token,docFreqMap));
            bw.newLine();
            nextLine(heapEntry,heap,mapper);
        }
        bw.flush();
        gos.finish();
    }

    private void nextLine(HeapEntry heapEntry, PriorityQueue<HeapEntry> heap, ObjectMapper mapper) throws IOException {
        try {
            String nextLine = heapEntry.reader.readLine();
            if (nextLine == null) {
//                LOGGER.log(Level.INFO, "Reached end of file");
                heapEntry.reader.close();
//                temp_counter++;
//                LOGGER.info("temp counter: " + temp_counter);
                return;
            }
            Map<String,Map<Integer,Integer>> keyValueIndex = mapper.readValue(nextLine, new TypeReference<>() {});
            String token = keyValueIndex.keySet().iterator().next();
            Map<Integer,Integer> docFreqMap = keyValueIndex.get(token);
            HeapEntry nextHeapEntry = new HeapEntry(token, docFreqMap, heapEntry.reader);
            heap.add(nextHeapEntry);
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.warning("Failed to advance to next line of the ndjson file due to :" + e);
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
