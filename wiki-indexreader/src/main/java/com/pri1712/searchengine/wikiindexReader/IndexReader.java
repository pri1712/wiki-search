package com.pri1712.searchengine.wikiindexReader;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;

import java.util.zip.GZIPInputStream;
import java.util.List;


public class IndexReader {
    private final Path indexedFilePath;
    private final Path  indexTokenOffset;
    ObjectMapper mapper = new ObjectMapper().configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false)
            .configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, true);

    public IndexReader(String indexedFilePath,String indexTokenOffset) {
        this.indexedFilePath = Paths.get(indexedFilePath);
        this.indexTokenOffset = Paths.get(indexTokenOffset);
    }

    public void readTokenIndex() throws IOException {
        FileInputStream fis = new FileInputStream(indexTokenOffset.toString());
        GZIPInputStream gis = new GZIPInputStream(fis);
        BufferedReader buffRead = new BufferedReader(new InputStreamReader(gis));
        mapper.readValue(buffRead,new TypeReference<List<TokenOffsetData>>() {});
    }

}
