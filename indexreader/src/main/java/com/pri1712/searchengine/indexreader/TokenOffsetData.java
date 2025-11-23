package com.pri1712.searchengine.indexreader;

public class TokenOffsetData {
    private String token;
    private long offset;
    public TokenOffsetData() {}
    public TokenOffsetData(String token, long offset) {
        this.token = token;
        this.offset = offset;
    }

    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }
}
