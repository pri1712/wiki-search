package com.pri1712.searchengine.wikitokenizer;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.pri1712.searchengine.wikiparser.BatchFileWriter;
import com.pri1712.searchengine.wikiparser.CheckpointManager;
import com.pri1712.searchengine.wikiutils.WikiDocument;

public class Tokenizer {
    private static final int MAX_BATCH_SIZE = 1;

    private static final Logger LOGGER = Logger.getLogger(Tokenizer.class.getName());
    ObjectMapper mapper = new ObjectMapper();
    TokenNormalizer tokenNormalizer = new TokenNormalizer();
    private int tokenizerBatchCounter = 0;
    private int previousTokenizerBatchCounter = 0;
    private final String tokenizerBatchCheckpointFile = "tokenizerCheckpoint.txt";
    private final CheckpointManager checkpointManager = new CheckpointManager(tokenizerBatchCheckpointFile);
    private final BatchFileWriter batchFileWriter = new BatchFileWriter("data/tokenized-data/");
    private List<TokenizedData> totalTokenizedData = new ArrayList<>();

    public Tokenizer() throws RuntimeException {
        this.previousTokenizerBatchCounter = checkpointManager.readCheckpointBatch();
    }

    public void tokenizeData(String parsedFilePath) {
        Path parsedPath = Paths.get(parsedFilePath);
        try (Stream<Path> fileStream = Files.list(parsedPath).filter(f -> f.toString().endsWith(".json.gz"))) {
            fileStream.forEach(file -> {
                try {
                   processFile(file);
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE,"IO exception while reading compressed json files",e);
                }
            });
        } catch (IOException e ) {
            throw new RuntimeException(e);
        }

    }

    private void processFile(Path file) throws IOException {
        try (FileInputStream fis = new FileInputStream(file.toString());
        GZIPInputStream gis = new GZIPInputStream(fis);
        BufferedReader buffRead = new BufferedReader(new InputStreamReader(gis))) {
            List<WikiDocument> jsonDocuments = mapper.readValue(buffRead, new TypeReference<List<WikiDocument>>() {});
            for (WikiDocument wikiDocument : jsonDocuments) {
//                System.out.printf("Title of the document is: %s %n", wikiDocument.getTitle());
                //normalize then tokenize.
                WikiDocument normalizedDocument =  tokenNormalizer.normalizeData(wikiDocument);
                TokenizedData tokenizedText = tokenNormalizer.tokenizeText(normalizedDocument); //tokenizedText now has the tokenized title and text for the document.
                totalTokenizedData.add(tokenizedText);
            }
            List<TokenizedData> newTokenizedData = new ArrayList<>(totalTokenizedData);
            totalTokenizedData.clear();
            LOGGER.info(String.format("Previous batch counter was %d and new batch counter is %d", previousTokenizerBatchCounter, tokenizerBatchCounter));
            if (previousTokenizerBatchCounter == -1 || previousTokenizerBatchCounter < tokenizerBatchCounter) {
                batchFileWriter.WriteBatch(newTokenizedData, tokenizerBatchCounter);
            }
            checkpointManager.writeCheckpointBatch(tokenizerBatchCounter);//store the checkpoint to a file.
            tokenizerBatchCounter++;

        }

    }

}