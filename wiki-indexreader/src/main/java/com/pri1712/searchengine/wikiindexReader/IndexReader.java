package com.pri1712.searchengine.wikiindexReader;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.zip.GZIPInputStream;
import java.util.List;


public class IndexReader {
    private final Path indexedFilePath;
    private final Path  indexTokenOffsetFilePath;
    ObjectMapper mapper = new ObjectMapper().configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false)
            .configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, true);

    public IndexReader(String indexedFilePath,String indexTokenOffsetFilePath) {
        this.indexedFilePath = Paths.get(indexedFilePath);
        this.indexTokenOffsetFilePath = Paths.get(indexTokenOffsetFilePath);
    }

    public void readTokenIndex(String token) throws IOException {
        FileInputStream fis = new FileInputStream(indexTokenOffsetFilePath.toString());
        GZIPInputStream gis = new GZIPInputStream(fis);
        BufferedReader buffRead = new BufferedReader(new InputStreamReader(gis, StandardCharsets.UTF_8));


    }

}
