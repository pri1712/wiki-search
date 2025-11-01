package com.pri1712.searchengine.wikitokenizer;

import java.util.List;

public class TokenizedData {
    private List<String> tokenizedText;
    private List<String> tokenizedTitle;
    private String Id;
    public TokenizedData(List<String> tokenizedText, List<String> tokenizedTitle, String Id) {
        this.tokenizedText = tokenizedText;
        this.tokenizedTitle = tokenizedTitle;
        this.Id = Id;
    }
    public TokenizedData() {}

    public String getId() {
        return Id;
    }

    public void setId(String id) {
        Id = id;
    }

    public List<String> getTokenizedText() {
        return tokenizedText;
    }

    public void setTokenizedText(List<String> tokenizedText) {
        this.tokenizedText = tokenizedText;
    }

    public List<String> getTokenizedTitle() {
        return tokenizedTitle;
    }

    public void setTokenizedTitle(List<String> tokenizedTitle) {
        this.tokenizedTitle = tokenizedTitle;
    }
}
