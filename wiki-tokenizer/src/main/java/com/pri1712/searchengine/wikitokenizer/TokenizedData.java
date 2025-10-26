package com.pri1712.searchengine.wikitokenizer;

import java.util.List;

public class TokenizedData {
    List<String> tokenizedText;
    List<String> tokenizedTitle;
    public TokenizedData(List<String> tokenizedText, List<String> tokenizedTitle) {
        this.tokenizedText = tokenizedText;
        this.tokenizedTitle = tokenizedTitle;
    }
}
