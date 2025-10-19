package com.pri1712.searchengine.wikiutils;

public class NormalizeText {

    public StringBuilder cleanText(StringBuilder text) {
        String lowercaseText = text.toString().toLowerCase();
        String cleanText = lowercaseText.replaceAll("[^a-z0-9\\s]"," ");
        cleanText = cleanText.replaceAll("\\s+", " ").trim();
        StringBuilder cleanSb =new StringBuilder(cleanText);
        return cleanSb;
    }

}
