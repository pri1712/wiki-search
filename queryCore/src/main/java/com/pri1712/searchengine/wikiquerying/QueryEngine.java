package com.pri1712.searchengine.wikiquerying;

public class QueryEngine {
    private String invertedIndex;
    private String docStats;
    private String tokenIndexOffset;
    public QueryEngine(String invertedIndex, String docStats, String tokenIndexOffset) {
        this.invertedIndex = invertedIndex;
        this.docStats = docStats;
        this.tokenIndexOffset = tokenIndexOffset;
    }
}
