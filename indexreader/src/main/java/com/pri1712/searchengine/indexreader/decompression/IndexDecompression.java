package com.pri1712.searchengine.indexreader.decompression;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.RandomAccessFile;

import java.nio.file.Path;
import java.util.*;

public class IndexDecompression {
    public IndexDecompression() {}
    //figure out how to decompress delta encoded index, probably easiest way would be to maintain a rolling sum.
    //how would this affect time complexity at read time?
    ObjectMapper mapper = new ObjectMapper().configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false)
            .configure(JsonParser.Feature.AUTO_CLOSE_SOURCE, true);

    public List<Map<Integer,Integer>> readCompressedIndex(Path indexfilePath, List<Long> tokenOffsets) throws IOException {
        //read data from the given offset, it returns a delta encoded list of docId and freq of the term in that docId.
        List<Map<Integer,Integer>> indexList = new ArrayList<>();
        RandomAccessFile indexRAF = new RandomAccessFile(indexfilePath.toFile(), "r");
        for (var offset : tokenOffsets) {
            if (offset == null || offset < 0) {
                indexList.add(Map.of());
            }
            indexRAF.seek(offset);
            decodeUTF8(indexList);
        }
    }

    private void decodeUTF8(List<Map<Integer,Integer>> indexList) throws IOException {
        //decode UTF8 manually since RAF reads in a different encoding format.

    }
}
