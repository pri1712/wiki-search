package com.pri1712.searchengine.wikiindexer.compression;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class IndexCompression {

    private static Logger LOGGER = Logger.getLogger(IndexCompression.class.getName());
    public IndexCompression() {}
    ObjectMapper mapper = new ObjectMapper().configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false)
            .configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, true);;

    public void deltaEncode(Path inputFilePath) {
        Path outputFilePath = Paths.get(inputFilePath.getParent().toString(),
                inputFilePath.getFileName().toString().replace(".json.gz", "_delta_encoded.json.gz")
        );
        try (FileInputStream fis = new FileInputStream(inputFilePath.toFile())) {
            GZIPInputStream gis = new GZIPInputStream(fis);
            BufferedReader br = new BufferedReader(new InputStreamReader(gis));
            GZIPOutputStream gos  = new GZIPOutputStream(new FileOutputStream(outputFilePath.toFile()));
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(gos));
            String line;
            while ((line = br.readLine()) != null){
                Map<String, Map<Integer,Integer>> index = mapper.readValue(line, new TypeReference<>() {});
                for (var e : index.entrySet()) {
                    String token = e.getKey();
                    Map<Integer,Integer> docFreqMap = e.getValue();
                    Map<Integer, Integer> deltaMap = new LinkedHashMap<>();
                    List<Integer> docIDs = new ArrayList<>(docFreqMap.keySet());
                    int firstDocID = docIDs.get(0);
                    int prevDocID = firstDocID;
                    deltaMap.put(firstDocID, docFreqMap.get(firstDocID));
                    for (int i = 1; i<docIDs.size(); i++) {
                        int currentDocID = docIDs.get(i);
                        int delta = currentDocID - prevDocID;
                        deltaMap.put(delta, docFreqMap.get(currentDocID));
                        prevDocID = currentDocID;
                    }
                    mapper.writeValue(bw, Map.of(token, deltaMap));
                    bw.newLine();
                }
            }
            bw.flush();
            gos.close();
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, "Could not open input file " + inputFilePath.toString(), e);
        }
    }
}
