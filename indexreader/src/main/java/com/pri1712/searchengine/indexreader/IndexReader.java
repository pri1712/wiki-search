package com.pri1712.searchengine.indexreader;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;


public class IndexReader {
    private static final Logger LOGGER = Logger.getLogger(String.valueOf(IndexReader.class));
    private final Path indexedFilePath;
    private final Path  indexTokenOffsetFilePath;
    private final Map<String,Long> tokenOffsetMap;
    ObjectMapper mapper = new ObjectMapper().configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false)
            .configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, true);

    public IndexReader(String indexedFilePath,String indexTokenOffsetFilePath) {
        this.indexedFilePath = Paths.get(indexedFilePath);
        this.indexTokenOffsetFilePath = Paths.get(indexTokenOffsetFilePath);
        tokenOffsetMap = new HashMap<>();
        try {
            loadTokenMapInMemory(indexTokenOffsetFilePath);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING,e.getMessage(),e);
        }
    }

    private void loadTokenMapInMemory(String indexTokenOffsetFilePath) throws IOException {
        FileInputStream fis = new FileInputStream(indexTokenOffsetFilePath.toString());
        GZIPInputStream gis = new GZIPInputStream(fis);
        BufferedReader buffRead = new BufferedReader(new InputStreamReader(gis, StandardCharsets.UTF_8));
        String line;
        while ((line = buffRead.readLine()) != null) {
            TokenOffsetData tokenOffsetData = mapper.readValue(line, TokenOffsetData.class);
            tokenOffsetMap.put(tokenOffsetData.getToken(), tokenOffsetData.getOffset());//storing token to offset in memory in a map.
        }
    }
    public void readTokenIndex(String token) throws IOException {
        Long tokenOffset = tokenOffsetMap.get(token);
        LOGGER.log(Level.INFO,"tokenOffset:"+tokenOffset);
    }

    public void readTokenIndex(List<String> tokens) throws IOException {
        for (String token : tokens) {
            Long tokenOffset = tokenOffsetMap.get(token);
        }
    }

}
