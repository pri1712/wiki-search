package com.pri1712.searchengine.wikiindexReader;

public class TokenOffsetData {
    private String token;
    private long offset;
    public TokenOffsetData() {}
    public TokenOffsetData(String token, long offset) {
        this.token = token;
        this.offset = offset;
    }
}
