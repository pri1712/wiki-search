package com.pri1712.searchengine.wikiindexer;

import java.util.Map;

public class HeapEntry {
    String token;
    Map<Integer,Integer> docFreq;
    public HeapEntry(String token, Map<Integer,Integer> docFreq) {
        this.token = token;
        this.docFreq = docFreq;
    }
}
