package com.pri1712.searchengine.wikiquerying;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public class QueryEngine {
    private static final Logger LOGGER = Logger.getLogger(String.valueOf(QueryEngine.class));
    private String invertedIndex;
    private String docStats;
    private String tokenIndexOffset;

    private Path indexedFilePath;
    public QueryEngine(String invertedIndex, String docStats, String tokenIndexOffset) throws IOException {
        this.invertedIndex = invertedIndex;
        this.docStats = docStats;
        this.tokenIndexOffset = tokenIndexOffset;
        Path directory = Paths.get(invertedIndex);
        this.indexedFilePath = Files.list(directory)
                .filter(p -> p.getFileName().toString().endsWith("_delta_encoded.json"))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("no inverted index found"));
    }

    public void preprocessQuery(String line) throws IOException {
        List<String> tokens = Arrays.asList(line.split(" "));
        LOGGER.info("tokens: " + tokens);
    }
}
