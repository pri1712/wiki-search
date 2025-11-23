package com.pri1712.searchengine.indexwriter;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Map;


public class HeapEntry {
    String token;
    Map<Integer,Integer> docFreq;
    BufferedReader reader;
    public HeapEntry(String token, Map<Integer,Integer> docFreq, BufferedReader reader) throws IOException {
        this.token = token;
        this.docFreq = docFreq;
        this.reader = reader;
    }
}
