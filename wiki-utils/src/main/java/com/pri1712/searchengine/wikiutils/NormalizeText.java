package com.pri1712.searchengine.wikiutils;

import java.util.logging.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NormalizeText {

    private static Logger LOGGER = Logger.getLogger(NormalizeText.class.getName());

    public StringBuilder cleanText(StringBuilder text) {
        if (text == null || text.isEmpty()) {
            return new StringBuilder();
        }
        String rawText = text.toString().trim();
        if ((rawText.matches("(?is)^#redirect\\s*\\[\\[.*?\\]\\]")) || rawText.matches("(?is)^#REDIRECT\\s*\\[\\[.*?\\]\\]")) {
//            LOGGER.log(Level.INFO,"Clean text: {0}", rawText);
            return new StringBuilder();
        }
        String lowercaseText = rawText.toLowerCase();
//        LOGGER.log(Level.INFO,"Clean text: {0}", lowercaseText);
        String cleanText = lowercaseText.replaceAll("[^a-z0-9\\s]"," ");
        cleanText = cleanText.replaceAll("\\s+", " ").trim();
        StringBuilder cleanSb =new StringBuilder(cleanText);
        return cleanSb;
    }

}
