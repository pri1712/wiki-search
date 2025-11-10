package com.pri1712.searchengine.wikiindexer.compression;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;

public class IndexDecompression {

    private static Logger LOGGER=Logger.getLogger(IndexDecompression.class.getName());
    ObjectMapper mapper=new ObjectMapper().configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET,false);

    public Map<Integer,Integer> decompressTokenMap(String token, Path inputFilePath){
        LOGGER.log(Level.INFO, String.format("decompressing token map for %s", token));
        try(FileInputStream fileInputStream=new FileInputStream(inputFilePath.toFile())){
            GZIPInputStream gzipInputStream=new GZIPInputStream(fileInputStream);
            BufferedReader bufferedReader=new BufferedReader(new InputStreamReader(gzipInputStream));

            String line;

            while((line=bufferedReader.readLine())!=null){
                Map<String, Map<Integer,Integer>> tempMap=mapper.readValue(line, new TypeReference<>(){});

                if(tempMap.containsKey(token)){
                    Map<Integer,Integer> indexMap=tempMap.get(token);
                    if(!indexMap.isEmpty()) return decompressionHelper(indexMap);
                }
            }

        }catch (IOException e){e.printStackTrace();}

        LOGGER.log(Level.INFO, "Decompression returned null");
        return null;
    }


    public Map<Integer,Integer> decompressionHelper(Map<Integer,Integer>indexMap){
        LOGGER.log(Level.FINE,"Decompression helper running");
        Map<Integer,Integer> decompressedMap=new TreeMap<>();
        List<Integer> sortedKeys=new ArrayList<>(indexMap.keySet());
        Collections.sort(sortedKeys);

        Integer prevDocId=sortedKeys.get(0);
        decompressedMap.put(prevDocId,indexMap.get(prevDocId));
        for(int i=1;i<sortedKeys.size();i++){
             Integer key=sortedKeys.get(i);
             Integer restoredDocId=prevDocId+key;
             decompressedMap.put(restoredDocId,indexMap.get(key));
             prevDocId=restoredDocId;
        }
        LOGGER.log(Level.FINE,"Decompression helper ran successfully");
        return decompressedMap;
    }
}
